use crate::model::{
    GatewayIdentityStatus, InboundRow, PlcLogDeviceRow, RequestContext, RequestItem,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tracing::info;

#[derive(Clone)]
pub struct DatabaseService {
    pub moqui_pool: PgPool,
    pub log_pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct ExportRow {
    pub rule_set_name: String,
    pub priority: i32,
    pub parameter_name: String,
    pub parameter_value: String,
}

impl DatabaseService {
    pub async fn init(moqui_url: &str, log_url: &str) -> Result<Self, sqlx::Error> {
        info!("Connecting to Moqui transactional DB: {}", moqui_url);
        let moqui_pool = PgPoolOptions::new()
            .max_connections(10)
            .min_connections(2)
            .connect(moqui_url)
            .await?;

        info!("Connecting to Moqui Log/Telemetry DB: {}", log_url);
        let log_pool = if log_url == moqui_url {
            moqui_pool.clone()
        } else {
            PgPoolOptions::new()
                .max_connections(8)
                .min_connections(1)
                .connect(log_url)
                .await?
        };

        Ok(Self {
            moqui_pool,
            log_pool,
        })
    }

    pub async fn inspect_gateway_identity(&self, gateway_device_id: &str) -> Result<GatewayIdentityStatus, sqlx::Error> {
        if gateway_device_id.trim().is_empty() {
            return Ok(GatewayIdentityStatus {
                gateway_device_id: "".to_string(),
                configured: crate::model::boolean_alias::Bool(false),
                physical_device_exists: false,
                gateway_member_found: false,
                message: "gateway.device.id / GATEWAY_DEVICE_ID is required.".to_string(),
            });
        }

        let physical_exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM DEVICE d
                JOIN PHYSICAL_DEVICE pd ON pd.DEVICE_ID = d.DEVICE_ID
                WHERE d.DEVICE_ID = $1
            )
            "#,
        )
        .bind(gateway_device_id)
        .fetch_one(&self.moqui_pool)
        .await
        .unwrap_or(false);

        if !physical_exists {
            return Ok(GatewayIdentityStatus {
                gateway_device_id: gateway_device_id.to_string(),
                configured: crate::model::boolean_alias::Bool(true),
                physical_device_exists: false,
                gateway_member_found: false,
                message: format!("Configured gateway.device.id {} does not exist as Device + PhysicalDevice.", gateway_device_id),
            });
        }

        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) AS CNT
            FROM DEVICE_GROUP_MEMBER dgm
            WHERE dgm.MEMBER_DEVICE_ID = $1
              AND dgm.PURPOSE_ENUM_ID = 'DgmpEdgeGateway'
              AND (dgm.STATUS_ID IS NULL OR dgm.STATUS_ID = 'DgmsAlive')
            "#,
        )
        .bind(gateway_device_id)
        .fetch_one(&self.moqui_pool)
        .await
        .unwrap_or(0);

        let gateway_member_found = count > 0;
        let message = if gateway_member_found {
            "ready".to_string()
        } else {
            format!("Gateway {} is not assigned to any DeviceGroup as DgmpEdgeGateway.", gateway_device_id)
        };

        Ok(GatewayIdentityStatus {
            gateway_device_id: gateway_device_id.to_string(),
            configured: crate::model::boolean_alias::Bool(true),
            physical_device_exists: true,
            gateway_member_found,
            message,
        })
    }

    pub async fn assert_gateway_scope(&self, gateway_device_id: &str, target_device_id: &str) -> Result<bool, sqlx::Error> {
        let count: i64 = sqlx::query_scalar(
            r#"
            WITH RECURSIVE root_groups (DEVICE_ID) AS (
                SELECT dgm.DEVICE_ID AS DEVICE_ID
                FROM DEVICE_GROUP_MEMBER dgm
                WHERE dgm.MEMBER_DEVICE_ID = $1
                  AND dgm.PURPOSE_ENUM_ID = 'DgmpEdgeGateway'
                  AND (dgm.STATUS_ID IS NULL OR dgm.STATUS_ID = 'DgmsAlive')
            ), visible_devices (DEVICE_ID) AS (
                SELECT rg.DEVICE_ID
                FROM root_groups rg
                UNION
                SELECT dgm.MEMBER_DEVICE_ID AS DEVICE_ID
                FROM DEVICE_GROUP_MEMBER dgm
                JOIN root_groups rg ON rg.DEVICE_ID = dgm.DEVICE_ID
                WHERE (dgm.STATUS_ID IS NULL OR dgm.STATUS_ID = 'DgmsAlive')
                UNION
                SELECT child.MEMBER_DEVICE_ID AS DEVICE_ID
                FROM DEVICE_GROUP_MEMBER child
                JOIN visible_devices vd ON vd.DEVICE_ID = child.DEVICE_ID
                WHERE (child.STATUS_ID IS NULL OR child.STATUS_ID = 'DgmsAlive')
            )
            SELECT COUNT(*) AS CNT
            FROM (SELECT DISTINCT DEVICE_ID FROM visible_devices) scoped
            WHERE scoped.DEVICE_ID = $2
            "#,
        )
        .bind(gateway_device_id)
        .bind(target_device_id)
        .fetch_one(&self.moqui_pool)
        .await?;

        Ok(count > 0)
    }

    pub async fn load_startup_subscription_request_names(&self, gateway_device_id: &str) -> Result<Vec<String>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            WITH RECURSIVE root_groups (DEVICE_ID) AS (
                SELECT dgm.DEVICE_ID AS DEVICE_ID
                FROM DEVICE_GROUP_MEMBER dgm
                WHERE dgm.MEMBER_DEVICE_ID = $1
                  AND dgm.PURPOSE_ENUM_ID = 'DgmpEdgeGateway'
                  AND (dgm.STATUS_ID IS NULL OR dgm.STATUS_ID = 'DgmsAlive')
            ), scoped_members (DEVICE_ID, MEMBER_DEVICE_ID, PURPOSE_ENUM_ID, STATUS_ID) AS (
                SELECT dgm.DEVICE_ID, dgm.MEMBER_DEVICE_ID, dgm.PURPOSE_ENUM_ID, dgm.STATUS_ID
                FROM DEVICE_GROUP_MEMBER dgm
                JOIN root_groups rg ON rg.DEVICE_ID = dgm.DEVICE_ID
                WHERE (dgm.STATUS_ID IS NULL OR dgm.STATUS_ID = 'DgmsAlive')
                UNION
                SELECT child.DEVICE_ID, child.MEMBER_DEVICE_ID, child.PURPOSE_ENUM_ID, child.STATUS_ID
                FROM DEVICE_GROUP_MEMBER child
                JOIN scoped_members sm ON sm.MEMBER_DEVICE_ID = child.DEVICE_ID
                WHERE (child.STATUS_ID IS NULL OR child.STATUS_ID = 'DgmsAlive')
            )
            SELECT startup.REQUEST_NAME AS "REQUEST_NAME"
            FROM (
                SELECT DISTINCT
                    dr.REQUEST_NAME,
                    COALESCE(dr.PRIORITY, 999999) AS SORT_PRIORITY,
                    COALESCE(dr.SEQUENCE_NUM, 999999) AS SORT_SEQUENCE
                FROM scoped_members sm
                JOIN DEVICE_REQUEST dr
                    ON dr.DEVICE_ID = sm.MEMBER_DEVICE_ID
                WHERE sm.PURPOSE_ENUM_ID IN (
                    'DgmpProcessPLC',
                    'DgmpSafetyPLC',
                    'DgmpController',
                    'DgmpMotionController',
                    'DgmpRemoteIO',
                    'DgmpSafetyRemoteIO',
                    'DgmpExtremeConditionRemoteIO'
                )
                  AND dr.ROUTER_ENUM_ID = 'DrrMoquiDeviceGateway'
                  AND dr.REQUEST_TYPE_ENUM_ID IN (
                      'DrtSubscribe',
                      'DrtEvent',
                      'DrtStateChange',
                      'DrtCyclic'
                  )
                  AND (dr.FROM_DATE IS NULL OR dr.FROM_DATE <= CURRENT_TIMESTAMP)
                  AND (dr.THRU_DATE IS NULL OR dr.THRU_DATE > CURRENT_TIMESTAMP)
            ) startup
            ORDER BY startup.SORT_PRIORITY, startup.SORT_SEQUENCE, startup.REQUEST_NAME
            "#,
        )
        .bind(gateway_device_id)
        .fetch_all(&self.moqui_pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.get::<String, _>("REQUEST_NAME")).collect())
    }

    pub async fn load_request_context(&self, request_name: &str) -> Result<RequestContext, sqlx::Error> {
        let req_row = sqlx::query(
            r#"
            SELECT dr.REQUEST_NAME AS "REQUEST_NAME", dr.DEVICE_ID AS "DEVICE_ID", dr.PARENT_REQUEST_NAME AS "PARENT_REQUEST_NAME", dr.REQUEST_TYPE_ENUM_ID AS "REQUEST_TYPE_ENUM_ID",
                dr.PURPOSE_ENUM_ID AS "PURPOSE_ENUM_ID", dr.ROUTER_ENUM_ID AS "ROUTER_ENUM_ID", dr.CONNECTION_NAME AS "CONNECTION_NAME", dr.BROKER_URI AS "BROKER_URI",
                CAST(dr.TIMEOUT AS INT4) AS "TIMEOUT", CAST(dr.POLLING_INTERVAL AS INT4) AS "POLLING_INTERVAL", dr.QUERY AS "QUERY", dr.ONLY_CHANGED_PARAMETERS AS "ONLY_CHANGED_PARAMETERS",
                dc.DRIVER_ENUM_ID AS "DRIVER_ENUM_ID", driver.ENUM_CODE AS "DRIVER_ENUM_CODE",
                dc.TRANSPORT_ENUM_ID AS "TRANSPORT_ENUM_ID", transport.ENUM_CODE AS "TRANSPORT_ENUM_CODE",
                dc.TRANSPORT_CONFIG AS "TRANSPORT_CONFIG", dc.OPTIONS AS "OPTIONS"
            FROM DEVICE_REQUEST dr
            LEFT JOIN DEVICE_CONNECTION dc ON dc.CONNECTION_NAME = dr.CONNECTION_NAME
            LEFT JOIN ENUMERATION driver ON driver.ENUM_ID = dc.DRIVER_ENUM_ID
            LEFT JOIN ENUMERATION transport ON transport.ENUM_ID = dc.TRANSPORT_ENUM_ID
            WHERE dr.REQUEST_NAME = $1
            "#,
        )
        .bind(request_name)
        .fetch_optional(&self.moqui_pool)
        .await?;

        let row = match req_row {
            Some(r) => r,
            None => {
                return Err(sqlx::Error::RowNotFound);
            }
        };

        let req_name: String = row.get("REQUEST_NAME");
        let device_id: Option<String> = row.get("DEVICE_ID");
        let parent_request_name: Option<String> = row.get("PARENT_REQUEST_NAME");
        let request_type_enum_id: String = row.get("REQUEST_TYPE_ENUM_ID");
        let purpose_enum_id: Option<String> = row.get("PURPOSE_ENUM_ID");
        let router_enum_id: Option<String> = row.get("ROUTER_ENUM_ID");
        let connection_name: Option<String> = row.get("CONNECTION_NAME");
        let broker_uri: Option<String> = row.get("BROKER_URI");
        let timeout: Option<i32> = row.get("TIMEOUT");
        let polling_interval: Option<i32> = row.get("POLLING_INTERVAL");
        let request_query: Option<String> = row.get("QUERY");
        let only_changed_parameters: Option<String> = row.get("ONLY_CHANGED_PARAMETERS");
        let driver_enum_id: Option<String> = row.get("DRIVER_ENUM_ID");
        let driver_enum_code: Option<String> = row.get("DRIVER_ENUM_CODE");
        let transport_enum_id: Option<String> = row.get("TRANSPORT_ENUM_ID");
        let transport_enum_code: Option<String> = row.get("TRANSPORT_ENUM_CODE");
        let transport_config: Option<String> = row.get("TRANSPORT_CONFIG");
        let options: Option<String> = row.get("OPTIONS");

        let items_rows = sqlx::query(
            r#"
            SELECT CAST(dri.SEQUENCE_NUM AS INT4) AS "SEQUENCE_NUM", dri.PARAMETER_ID AS "PARAMETER_ID", dri.REQUEST_ITEM_NAME AS "REQUEST_ITEM_NAME",
                p.NUMERIC_VALUE AS "NUMERIC_VALUE", p.SYMBOLIC_VALUE AS "SYMBOLIC_VALUE", p.PARAMETER_ENUM_ID AS "PARAMETER_ENUM_ID",
                COALESCE(dri.QUERY, dr.QUERY) AS "TARGET_QUERY"
            FROM DEVICE_REQUEST dr
            INNER JOIN DEVICE_REQUEST_ITEM dri ON dr.REQUEST_NAME = dri.REQUEST_NAME
            INNER JOIN PARAMETER p ON p.PARAMETER_ID = dri.PARAMETER_ID
            LEFT JOIN ENTITY_AUDIT_LOG eal
                ON eal.PK_PRIMARY_VALUE = dri.PARAMETER_ID
                AND eal.CHANGED_ENTITY_NAME = 'moqui.math.Parameter'
                AND eal.CHANGED_DATE > dr.LAST_UPDATED_STAMP
            WHERE dr.REQUEST_NAME = $1
                AND (dr.REQUEST_TYPE_ENUM_ID <> 'DrtWrite'
                    OR dr.ONLY_CHANGED_PARAMETERS <> 'Y'
                    OR eal.PK_PRIMARY_VALUE IS NOT NULL)
            ORDER BY dri.SEQUENCE_NUM, dri.PARAMETER_ID
            "#,
        )
        .bind(request_name)
        .fetch_all(&self.moqui_pool)
        .await?;

        let items = items_rows
            .into_iter()
            .map(|r| {
                let numeric: Option<rust_decimal::Decimal> = r.try_get("NUMERIC_VALUE").ok();
                RequestItem {
                    parameter_id: r.get("PARAMETER_ID"),
                    request_item_name: r.get("REQUEST_ITEM_NAME"),
                    sequence_num: r.get("SEQUENCE_NUM"),
                    numeric_value: numeric.and_then(|d| d.to_string().parse::<f64>().ok()),
                    symbolic_value: r.get("SYMBOLIC_VALUE"),
                    parameter_enum_id: r.get("PARAMETER_ENUM_ID"),
                    target_query: r.get("TARGET_QUERY"),
                }
            })
            .collect();

        Ok(RequestContext {
            request_name: req_name,
            device_id,
            parent_request_name,
            request_type_enum_id,
            purpose_enum_id,
            router_enum_id,
            connection_name,
            broker_uri,
            timeout,
            polling_interval,
            request_query,
            only_changed_parameters,
            driver_enum_id,
            driver_enum_code,
            transport_enum_id,
            transport_enum_code,
            transport_config,
            options,
            items,
        })
    }

    pub async fn upsert_parameter_batch(&self, rows: &[InboundRow]) -> Result<(), sqlx::Error> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut tx = self.moqui_pool.begin().await?;
        for row in rows {
            let dec_val: Option<rust_decimal::Decimal> = row
                .numeric_value
                .and_then(|v| rust_decimal::Decimal::from_str_exact(&v.to_string()).ok());

            sqlx::query(
                r#"
                UPDATE PARAMETER SET
                    NUMERIC_VALUE = COALESCE($1, PARAMETER.NUMERIC_VALUE),
                    SYMBOLIC_VALUE = COALESCE($2, PARAMETER.SYMBOLIC_VALUE),
                    PARAMETER_ENUM_ID = COALESCE($3, PARAMETER.PARAMETER_ENUM_ID),
                    LAST_UPDATED_STAMP = CURRENT_TIMESTAMP
                WHERE PARAMETER_ID = $4
                "#,
            )
            .bind(dec_val)
            .bind(&row.symbolic_value)
            .bind(&row.parameter_enum_id)
            .bind(&row.parameter_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn insert_parameter_log_batch(&self, rows: &[InboundRow]) -> Result<(), sqlx::Error> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut tx = self.log_pool.begin().await?;
        let base_seq = jiff::Timestamp::now().as_millisecond();
        for (idx, row) in rows.iter().enumerate() {
            let log_id = row
                .parameter_log_id
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let seq_num = row.sequence_num.unwrap_or(base_seq + idx as i64);

            let dec_val: Option<rust_decimal::Decimal> = row
                .numeric_value
                .and_then(|v| rust_decimal::Decimal::from_str_exact(&v.to_string()).ok());

            let observed_str = row
                .observed_date
                .map(|t| t.to_string());

            sqlx::query(
                r#"
                INSERT INTO PARAMETER_LOG
                    (PARAMETER_LOG_ID, PARAMETER_ID, SEQUENCE_NUM, OBSERVED_DATE,
                     NUMERIC_VALUE, SYMBOLIC_VALUE, PARAMETER_ENUM_ID)
                VALUES
                    ($1, $2, $3,
                     COALESCE(CAST($4 AS TIMESTAMPTZ), CURRENT_TIMESTAMP),
                     $5, $6, $7)
                "#,
            )
            .bind(&log_id)
            .bind(&row.parameter_id)
            .bind(seq_num)
            .bind(observed_str)
            .bind(dec_val)
            .bind(&row.symbolic_value)
            .bind(&row.parameter_enum_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn insert_device_log_batch(&self, rows: &[PlcLogDeviceRow]) -> Result<(), sqlx::Error> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut tx = self.log_pool.begin().await?;
        for row in rows {
            let observed_str = row.observed_date.map(|t| t.to_string());

            sqlx::query(
                r#"
                INSERT INTO DEVICE_LOG
                    (DEVICE_LOG_ID, DEVICE_ID, PAYLOAD, SEQUENCE_NUM, OBSERVED_DATE)
                VALUES
                    ($1, $2, $3, $4,
                     COALESCE(CAST($5 AS TIMESTAMPTZ), CURRENT_TIMESTAMP))
                "#,
            )
            .bind(&row.device_log_id)
            .bind(&row.device_id)
            .bind(&row.payload)
            .bind(row.sequence_num)
            .bind(observed_str)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn export_device_config(
        &self,
        rule_set_id: &str,
        device_id: Option<&str>,
        device_rule_id: Option<&str>,
    ) -> Result<Vec<ExportRow>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            WITH base AS (
                SELECT drs.RULE_SET_NAME, dr.PRIORITY, dc.DEVICE_CONFIG_ID,
                       dc.DEVICE_TYPE_ENUM_ID,
                       dc.APPROXIMATED_FUNCTION_ID,
                       COALESCE(phdev.DEVICE_NAME, dr.DEVICE_ID) AS DEVICE_NAME
                FROM DEVICE_RULE_SET drs
                INNER JOIN DEVICE_RULE dr ON drs.DEVICE_RULE_SET_ID = dr.DEVICE_RULE_SET_ID
                INNER JOIN DEVICE_CONFIG dc ON dr.DEVICE_CONFIG_ID = dc.DEVICE_CONFIG_ID
                LEFT JOIN PHYSICAL_DEVICE phdev ON phdev.DEVICE_ID = dr.DEVICE_ID
                WHERE drs.DEVICE_RULE_SET_ID = $1
                    AND (CAST($2 AS VARCHAR) IS NULL OR dr.DEVICE_ID = $2)
                    AND (CAST($3 AS VARCHAR) IS NULL OR dr.DEVICE_RULE_ID = $3)
            ), pt_base AS (
                SELECT b.RULE_SET_NAME, b.PRIORITY,
                       tp.APPROXIMATED_FUNCTION_ID, tp.APPROXIMATED_FUNCTION_SAMPLE_ID,
                       afs.SEQUENCE_NUM, afs.SAMPLE_VECTOR_ID,
                       tp.IS_BREAK_POINT, tp.BLENDING_ENUM_ID, tp.POINT_TIME_OFFSET_MILLIS
                FROM base b
                INNER JOIN TRAJECTORY_POINT tp
                    ON tp.APPROXIMATED_FUNCTION_ID = b.APPROXIMATED_FUNCTION_ID
                INNER JOIN APPROXIMATED_FUNCTION_SAMPLE afs
                    ON afs.APPROXIMATED_FUNCTION_ID = tp.APPROXIMATED_FUNCTION_ID
                    AND afs.APPROXIMATED_FUNCTION_SAMPLE_ID = tp.APPROXIMATED_FUNCTION_SAMPLE_ID
                WHERE b.APPROXIMATED_FUNCTION_ID IS NOT NULL
            )
            SELECT
                b.RULE_SET_NAME AS "ruleSetName",
                b.PRIORITY AS "priority",
                COALESCE(
                    NULLIF(MAX(dri.REQUEST_ITEM_NAME), ''),
                    CASE
                        WHEN b.DEVICE_TYPE_ENUM_ID = 'DtPLC' THEN
                            'dev.' || COALESCE(NULLIF(p.PARAMETER_ALIAS, ''), pd.PARAMETER_NAME)
                        WHEN b.DEVICE_TYPE_ENUM_ID LIKE 'DtMoquiPlc%' THEN
                            'dev.' || b.DEVICE_NAME || '.' || COALESCE(NULLIF(p.PARAMETER_ALIAS, ''), pd.PARAMETER_NAME)
                        ELSE COALESCE(NULLIF(p.PARAMETER_ALIAS, ''), b.DEVICE_NAME || '.' || pd.PARAMETER_NAME)
                    END
                ) AS "parameterName",
                CASE
                    WHEN p.NUMERIC_VALUE     IS NOT NULL THEN CAST(p.NUMERIC_VALUE AS VARCHAR(50))
                    WHEN p.PARAMETER_ENUM_ID IS NOT NULL THEN p.PARAMETER_ENUM_ID
                    ELSE p.SYMBOLIC_VALUE
                END AS "parameterValue"
            FROM base b
            INNER JOIN PARAMETER p ON b.DEVICE_CONFIG_ID = p.DEVICE_CONFIG_ID
            INNER JOIN PARAMETER_DEF pd ON p.PARAMETER_DEF_ID = pd.PARAMETER_DEF_ID
            LEFT JOIN DEVICE_REQUEST_ITEM dri ON dri.PARAMETER_ID = p.PARAMETER_ID
            GROUP BY b.RULE_SET_NAME, b.PRIORITY, p.PARAMETER_ID, p.PARAMETER_ALIAS,
                     p.NUMERIC_VALUE, p.PARAMETER_ENUM_ID, p.SYMBOLIC_VALUE,
                     b.DEVICE_NAME, b.DEVICE_TYPE_ENUM_ID, pd.PARAMETER_NAME, pd.PARAMETER_CODE
            UNION ALL
            SELECT RULE_SET_NAME, PRIORITY,
                'Trajectory.trajectoryId', CAST(APPROXIMATED_FUNCTION_ID AS VARCHAR)
            FROM base WHERE APPROXIMATED_FUNCTION_ID IS NOT NULL
            UNION ALL
            SELECT RULE_SET_NAME, PRIORITY,
                'Trajectory.pointCount', CAST(COUNT(*) AS VARCHAR)
            FROM pt_base GROUP BY RULE_SET_NAME, PRIORITY
            UNION ALL
            SELECT pb.RULE_SET_NAME, pb.PRIORITY,
                'Trajectory.points[' || (pb.SEQUENCE_NUM - 1) || '].pos.v[' || vc.DIMENSION_INDEX || ']',
                CAST(vc.REAL_VALUE AS VARCHAR)
            FROM pt_base pb
            INNER JOIN VECTOR v ON v.VECTOR_ID = pb.SAMPLE_VECTOR_ID
            INNER JOIN VECTOR_COMPONENT vc ON vc.VECTOR_ID = v.VECTOR_ID
            UNION ALL
            SELECT RULE_SET_NAME, PRIORITY,
                'Trajectory.points[' || (SEQUENCE_NUM - 1) || '].sequenceNum',
                CAST(SEQUENCE_NUM AS VARCHAR)
            FROM pt_base
            UNION ALL
            SELECT RULE_SET_NAME, PRIORITY,
                'Trajectory.points[' || (SEQUENCE_NUM - 1) || '].timeOffsetMs',
                CAST(COALESCE(POINT_TIME_OFFSET_MILLIS, 0.0) AS VARCHAR)
            FROM pt_base
            UNION ALL
            SELECT RULE_SET_NAME, PRIORITY,
                'Trajectory.points[' || (SEQUENCE_NUM - 1) || '].isBreakPoint',
                CASE WHEN IS_BREAK_POINT = 'Y' THEN 'TRUE' ELSE 'FALSE' END
            FROM pt_base
            UNION ALL
            SELECT RULE_SET_NAME, PRIORITY,
                'Trajectory.points[' || (SEQUENCE_NUM - 1) || '].hasBlendingOverride',
                CASE WHEN BLENDING_ENUM_ID IS NOT NULL THEN 'TRUE' ELSE 'FALSE' END
            FROM pt_base
            UNION ALL
            SELECT RULE_SET_NAME, PRIORITY,
                'Trajectory.points[' || (SEQUENCE_NUM - 1) || '].bufferMode',
                CASE BLENDING_ENUM_ID
                    WHEN 'PpcmPlcAborting'     THEN '0'
                    WHEN 'PpcmPlcBuffered'     THEN '1'
                    WHEN 'PpcmPlcBlendingLow'  THEN '2'
                    WHEN 'PpcmPlcBlendingPrev' THEN '3'
                    WHEN 'PpcmPlcBlendingNext' THEN '4'
                    WHEN 'PpcmPlcBlendingHigh' THEN '5'
                    ELSE '3'
                END
            FROM pt_base
            UNION ALL
            SELECT RULE_SET_NAME, PRIORITY,
                'Trajectory.points[' || (SEQUENCE_NUM - 1) || '].transitionMode',
                CASE BLENDING_ENUM_ID
                    WHEN 'PpcmPlcTMNone'       THEN '0'
                    WHEN 'PpcmPlcTMCornerDist' THEN '1'
                    WHEN 'PpcmPlcTMConstVel'   THEN '2'
                    WHEN 'PpcmPlcTMStartVel'   THEN '3'
                    WHEN 'PpcmPlcTMMaxVel'     THEN '4'
                    ELSE '0'
                END
            FROM pt_base
            UNION ALL
            SELECT pb.RULE_SET_NAME, pb.PRIORITY,
                'Trajectory.points[' || (pb.SEQUENCE_NUM - 1) || '].blendingMode',
                CAST(COALESCE(ppp.TOLERANCE, 0.0) AS VARCHAR)
            FROM pt_base pb
            LEFT JOIN PARAMETRIC_PATH_POINT ppp
                ON ppp.APPROXIMATED_FUNCTION_ID = pb.APPROXIMATED_FUNCTION_ID
                AND ppp.APPROXIMATED_FUNCTION_SAMPLE_ID = pb.APPROXIMATED_FUNCTION_SAMPLE_ID
            ORDER BY "priority", "parameterName"
            "#,
        )
        .bind(rule_set_id)
        .bind(device_id)
        .bind(device_rule_id)
        .fetch_all(&self.moqui_pool)
        .await?;

        let result = rows
            .into_iter()
            .map(|r| ExportRow {
                rule_set_name: r.get("ruleSetName"),
                priority: r.get("priority"),
                parameter_name: r.get("parameterName"),
                parameter_value: r.get("parameterValue"),
            })
            .collect();

        Ok(result)
    }
}
