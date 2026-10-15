use crate::db::ExportRow;
use crate::model::ExportFileSpec;
use std::collections::BTreeMap;

pub struct RecipeExporter;

impl RecipeExporter {
    pub fn build_export_files(rows: &[ExportRow]) -> Vec<ExportFileSpec> {
        if rows.is_empty() {
            return Vec::new();
        }

        let rule_set_name = &rows[0].rule_set_name;
        let mut by_priority: BTreeMap<i32, Vec<&ExportRow>> = BTreeMap::new();

        for row in rows {
            by_priority.entry(row.priority).or_default().push(row);
        }

        by_priority
            .into_iter()
            .map(|(priority, group)| {
                let content = group
                    .iter()
                    .map(|r| format!("{}:={}", r.parameter_name, r.parameter_value))
                    .collect::<Vec<_>>()
                    .join("\n");

                let filename = format!("{}_p{:02}.txt", rule_set_name, priority);
                let row_count = group.len();

                ExportFileSpec {
                    filename,
                    content,
                    priority,
                    row_count,
                }
            })
            .collect()
    }
}
