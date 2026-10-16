use crate::config::AppConfig;
use crate::export::RecipeExporter;
use crate::protocols::FileTransferService;
use crate::supervisor::SubscriptionManager;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use base64::Engine;
use serde::Deserialize;
use serde_json::json;
use tracing::warn;

#[derive(Clone)]
pub struct AppState {
    pub manager: SubscriptionManager,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/api/device-request/run/{request_name}", post(run_device_request))
        .route("/api/device-content/transfer/{request_name}", post(transfer_device_content))
        .route("/api/device-config/export", post(export_device_config))
        .route("/q/health", get(health_check))
        .route("/q/health/ready", get(health_check))
        .route("/q/health/live", get(health_live))
        .route("/health/ready", get(health_check))
        .with_state(state)
}

fn authorize(headers: &HeaderMap, config: &AppConfig) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if !config.api_auth_enabled || config.is_safe_dev_profile() {
        return Ok(());
    }

    let token_from_custom_header = headers
        .get(&config.api_auth_header)
        .and_then(|v| v.to_str().ok());

    let token_from_auth_header = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer ").map(|t| t.trim()));

    let provided = token_from_custom_header.or(token_from_auth_header);

    match provided {
        Some(t) if t == config.api_auth_token => Ok(()),
        _ => Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "unauthorized",
                "message": "Missing or invalid API credential."
            })),
        )),
    }
}

async fn run_device_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(request_name): Path<String>,
) -> impl IntoResponse {
    if let Err(auth_err) = authorize(&headers, state.manager.config()) {
        return auth_err.into_response();
    }

    let context = match state.manager.db().load_request_context(&request_name).await {
        Ok(ctx) => ctx,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "not_found", "message": format!("DeviceRequest {} not found: {}", request_name, e)})),
            )
                .into_response();
        }
    };

    if let Some(dev_id) = &context.device_id {
        let gw_id = &state.manager.config().gateway_device_id;
        if !gw_id.is_empty() {
            match state.manager.db().assert_gateway_scope(gw_id, dev_id).await {
                Ok(true) => {}
                Ok(false) => {
                    return (
                        StatusCode::FORBIDDEN,
                        Json(json!({
                            "error": "forbidden",
                            "message": format!("DeviceRequest {} belongs to device {}, which is not in the scope of gateway {}.", request_name, dev_id, gw_id)
                        })),
                    )
                        .into_response();
                }
                Err(e) => {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error": "service_unavailable", "message": e.to_string()})),
                    )
                        .into_response();
                }
            }
        }
    }

    match state.manager.dispatch(&context).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "dispatch_failed", "message": e})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct TransferContentBody {
    filename: Option<String>,
    #[serde(rename = "contentBase64")]
    content_base64: Option<String>,
}

async fn transfer_device_content(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(request_name): Path<String>,
    Json(body): Json<TransferContentBody>,
) -> impl IntoResponse {
    if let Err(auth_err) = authorize(&headers, state.manager.config()) {
        return auth_err.into_response();
    }

    let (filename, content_base64) = match (body.filename, body.content_base64) {
        (Some(f), Some(c)) => (f, c),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "filename and contentBase64 are required"})),
            )
                .into_response();
        }
    };

    let context = match state.manager.db().load_request_context(&request_name).await {
        Ok(ctx) => ctx,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "not_found", "message": e.to_string()})),
            )
                .into_response();
        }
    };

    let broker_uri = match &context.broker_uri {
        Some(u) if !u.trim().is_empty() => u.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("DeviceRequest {} has no brokerUri (transfer destination).", request_name)})),
            )
                .into_response();
        }
    };

    let file_bytes = match base64::engine::general_purpose::STANDARD.decode(&content_base64) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Invalid base64 payload: {}", e)})),
            )
                .into_response();
        }
    };

    match FileTransferService::transfer_bytes(&broker_uri, &filename, &file_bytes).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({
                "status": "completed",
                "requestName": request_name,
                "filename": filename,
                "bytes": file_bytes.len(),
                "transferUri": broker_uri
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "transfer_failed", "message": e})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct ExportConfigBody {
    #[serde(rename = "deviceRuleSetId")]
    device_rule_set_id: Option<String>,
    #[serde(rename = "deviceId")]
    device_id: Option<String>,
    #[serde(rename = "deviceRuleId")]
    device_rule_id: Option<String>,
}

async fn export_device_config(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ExportConfigBody>,
) -> impl IntoResponse {
    if let Err(auth_err) = authorize(&headers, state.manager.config()) {
        return auth_err.into_response();
    }

    let rule_set_id = match body.device_rule_set_id {
        Some(id) if !id.trim().is_empty() => id,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "deviceRuleSetId is required"})),
            )
                .into_response();
        }
    };

    let rows = match state
        .manager
        .db()
        .export_device_config(&rule_set_id, body.device_id.as_deref(), body.device_rule_id.as_deref())
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "query_failed", "message": e.to_string()})),
            )
                .into_response();
        }
    };

    let files = RecipeExporter::build_export_files(&rows);
    let dest_uri = &state.manager.config().file_transfer_uri;

    let mut transfer_summaries = Vec::new();
    for file_spec in &files {
        if let Err(e) = FileTransferService::transfer_text(dest_uri, &file_spec.filename, &file_spec.content).await {
            warn!("Failed to transfer recipe file {}: {}", file_spec.filename, e);
        }
        if state.manager.config().file_transfer_uri_2_enabled
            && let Some(dest2) = &state.manager.config().file_transfer_uri_2 {
                let _ = FileTransferService::transfer_text(dest2, &file_spec.filename, &file_spec.content).await;
            }
        transfer_summaries.push(json!({
            "filename": file_spec.filename,
            "priority": file_spec.priority,
            "rowCount": file_spec.row_count
        }));
    }

    (
        StatusCode::OK,
        Json(json!({
            "status": "completed",
            "routeId": "run-device-config-export",
            "files": transfer_summaries
        })),
    )
        .into_response()
}

async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    let gw_id = &state.manager.config().gateway_device_id;
    let identity = state.manager.db().inspect_gateway_identity(gw_id).await;

    let (status_str, is_ready, identity_status) = match identity {
        Ok(id) => {
            let ready = id.ready();
            (if ready { "UP" } else { "DOWN" }, ready, Some(id))
        }
        Err(_) => ("DOWN", false, None),
    };

    let code = if is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let data = json!({
        "status": status_str,
        "checks": [
            {
                "name": "gateway-subscriptions",
                "status": status_str,
                "data": {
                    "gatewayDeviceId": gw_id,
                    "datasource": "UP",
                    "gatewayConfigured": identity_status.as_ref().map(|s| s.configured.0).unwrap_or(false),
                    "physicalDeviceExists": identity_status.as_ref().map(|s| s.physical_device_exists).unwrap_or(false),
                    "gatewayMembershipValid": identity_status.as_ref().map(|s| s.gateway_member_found).unwrap_or(false),
                    "message": identity_status.as_ref().map(|s| s.message.clone()).unwrap_or_else(|| "DB error".to_string()),
                    "activeRoutes": state.manager.active_route_count().await
                }
            }
        ]
    });

    (code, Json(data))
}

async fn health_live() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "status": "UP",
            "checks": []
        })),
    )
}
