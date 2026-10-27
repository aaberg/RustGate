use std::io::{self, Write};

use serde_json::Value;

const DEFAULT_GENERATION_TOKENS: i64 = 4096;
const PROMPT_OVERHEAD: i64 = 256;
const MESSAGE_OVERHEAD: i64 = 32;

pub(super) enum BudgetError {
    Invalid,
    Unsupported,
    Insufficient,
}

// Count the canonical request without allocating another copy of its contents.
struct ByteCount(i64);

impl Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len().try_into().map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("request size overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn positive_integer(value: &Value) -> Result<i64, BudgetError> {
    value
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or(BudgetError::Invalid)
}

/// Byte bounds apply only to text tokenization, never remote images, audio,
/// files, or provider-managed tools whose cost is not represented in the body.
fn text_only(body: &Value) -> bool {
    if [
        "audio",
        "web_search_options",
        "input_audio",
        "image",
        "video",
        "file",
    ]
    .iter()
    .any(|field| body.get(field).is_some())
    {
        return false;
    }
    if let Some(modalities) = body.get("modalities")
        && !modalities.as_array().is_some_and(|modalities| {
            modalities
                .iter()
                .all(|modality| modality.as_str() == Some("text"))
        })
    {
        return false;
    }
    if let Some(tools) = body.get("tools")
        && !tools.as_array().is_some_and(|tools| {
            tools
                .iter()
                .all(|tool| tool.get("type").and_then(Value::as_str) == Some("function"))
        })
    {
        return false;
    }
    body["messages"].as_array().is_some_and(|messages| {
        messages.iter().all(|message| {
            message.is_object()
                && message.get("audio").is_none()
                && match message.get("content") {
                    None | Some(Value::Null | Value::String(_)) => true,
                    Some(Value::Array(parts)) => parts.iter().all(|part| {
                        part.get("type").and_then(Value::as_str) == Some("text")
                            && part.get("text").is_some_and(Value::is_string)
                    }),
                    _ => false,
                }
        })
    })
}

/// Constrain text chat to its admission escrow. The trusted upstream must use
/// byte-based text tokenization within this bound and honor the generation cap,
/// including reasoning tokens. Actual reports are never truncated to estimates.
pub(super) fn constrain(body: &mut Value, available: i64) -> Result<i64, BudgetError> {
    if !text_only(body) {
        return Err(BudgetError::Unsupported);
    }
    let choices = body
        .get("n")
        .map(positive_integer)
        .transpose()?
        .unwrap_or(1);
    let object = body.as_object_mut().ok_or(BudgetError::Invalid)?;
    if object.contains_key("max_tokens") && object.contains_key("max_completion_tokens") {
        return Err(BudgetError::Invalid);
    }
    let limit_field = if object.contains_key("max_completion_tokens") {
        "max_completion_tokens"
    } else {
        "max_tokens"
    };
    let requested = object
        .remove(limit_field)
        .as_ref()
        .map(positive_integer)
        .transpose()?
        .unwrap_or(DEFAULT_GENERATION_TOKENS);
    let mut bytes = ByteCount(0);
    serde_json::to_writer(&mut bytes, &body).map_err(|_| BudgetError::Invalid)?;
    let messages: i64 = body["messages"]
        .as_array()
        .unwrap()
        .len()
        .try_into()
        .map_err(|_| BudgetError::Invalid)?;
    let prompt = messages
        .checked_mul(MESSAGE_OVERHEAD)
        .and_then(|overhead| overhead.checked_add(PROMPT_OVERHEAD))
        .and_then(|overhead| bytes.0.checked_add(overhead))
        .ok_or(BudgetError::Invalid)?;
    let room = available
        .checked_sub(prompt)
        .filter(|room| *room >= choices)
        .ok_or(BudgetError::Insufficient)?;
    let generation = requested.min(room / choices);
    let reserved = generation
        .checked_mul(choices)
        .and_then(|generation| prompt.checked_add(generation))
        .ok_or(BudgetError::Invalid)?;
    body.as_object_mut()
        .unwrap()
        .insert(limit_field.to_owned(), generation.into());
    Ok(reserved)
}

/// Embeddings generate no completion tokens. Bound each text/token input by
/// canonical request bytes plus framing, without changing the client payload.
pub(super) fn embeddings(body: &Value, available: i64) -> Result<i64, BudgetError> {
    if !body
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(|model| !model.trim().is_empty())
        || [
            "stream",
            "stream_options",
            "max_tokens",
            "max_completion_tokens",
            "n",
        ]
        .iter()
        .any(|field| body.get(field).is_some())
        || body
            .get("encoding_format")
            .is_some_and(|value| !matches!(value.as_str(), Some("float" | "base64")))
        || body
            .get("dimensions")
            .is_some_and(|value| positive_integer(value).is_err())
        || body.get("user").is_some_and(|value| !value.is_string())
    {
        return Err(BudgetError::Invalid);
    }
    fn text(value: &Value) -> bool {
        value.as_str().is_some_and(|text| !text.is_empty())
    }
    fn token(value: &Value) -> bool {
        value
            .as_u64()
            .is_some_and(|token| u32::try_from(token).is_ok())
    }
    fn tokens(value: &Value) -> bool {
        value
            .as_array()
            .is_some_and(|values| !values.is_empty() && values.iter().all(token))
    }
    let input = body.get("input").ok_or(BudgetError::Invalid)?;
    let inputs = if text(input) || tokens(input) {
        1
    } else if let Some(inputs) = input.as_array()
        && !inputs.is_empty()
        && (inputs.iter().all(text) || inputs.iter().all(tokens))
    {
        i64::try_from(inputs.len()).map_err(|_| BudgetError::Invalid)?
    } else {
        return Err(BudgetError::Invalid);
    };
    let mut bytes = ByteCount(0);
    serde_json::to_writer(&mut bytes, body).map_err(|_| BudgetError::Invalid)?;
    let reserved = inputs
        .checked_mul(MESSAGE_OVERHEAD)
        .and_then(|overhead| overhead.checked_add(PROMPT_OVERHEAD))
        .and_then(|overhead| overhead.checked_add(bytes.0))
        .ok_or(BudgetError::Invalid)?;
    if reserved > available {
        return Err(BudgetError::Insufficient);
    }
    Ok(reserved)
}
