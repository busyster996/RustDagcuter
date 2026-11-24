use super::Scheduler;
use crate::{JoinPolicy, ON_SUCCESS};
use std::collections::HashSet;
use std::io::{self, Write};

impl Scheduler {
    /// Numbered list of successful tasks in recorded completion order.
    pub fn success_completion_order(&self) -> String {
        let run = self.run.lock().unwrap();
        let mut order = String::from("\n");
        for (index, name) in run.order.iter().enumerate() {
            order.push_str(&format!("{}. {}\n", index + 1, name));
        }
        order
    }

    pub fn write_graph(&self, writer: &mut dyn Write) -> io::Result<()> {
        let mut roots: Vec<_> = self
            .dependencies
            .iter()
            .filter(|(_, dependencies)| dependencies.edges.is_empty())
            .map(|(name, _)| name.as_str())
            .collect();
        roots.sort_unstable();

        let mut expanded = HashSet::new();
        for root in roots {
            writeln!(writer, "{root}")?;
            self.write_children(writer, root, "  ", &mut expanded)?;
            writeln!(writer)?;
        }
        Ok(())
    }

    pub fn print_graph(&self) {
        let stdout = io::stdout();
        let _ = self.write_graph(&mut stdout.lock());
    }

    fn write_children(
        &self,
        writer: &mut dyn Write,
        name: &str,
        prefix: &str,
        expanded: &mut HashSet<String>,
    ) -> io::Result<()> {
        if let Some(children) = self.dependents.get(name) {
            for edge in children {
                let child = &edge.to;
                let mut label = child.clone();
                if edge.on != ON_SUCCESS {
                    label.push_str(&format!(" [on {}]", edge.on));
                }
                if self.dependencies[child].join == JoinPolicy::AnyOf {
                    label.push_str(" (any-of)");
                }
                if !expanded.insert(child.clone()) {
                    writeln!(writer, "{prefix}└─> {label} ...")?;
                    continue;
                }
                writeln!(writer, "{prefix}└─> {label}")?;
                self.write_children(writer, child, &format!("{prefix}    "), expanded)?;
            }
        }
        Ok(())
    }
}
