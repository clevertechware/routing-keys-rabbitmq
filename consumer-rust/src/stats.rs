use std::collections::HashMap;

/// Tracks how many messages were received per routing key.
#[derive(Default)]
pub struct Stats {
    counts: HashMap<String, u64>,
}

impl Stats {
    pub fn record(&mut self, routing_key: &str) {
        *self.counts.entry(routing_key.to_string()).or_insert(0) += 1;
    }

    pub fn total(&self) -> u64 {
        self.counts.values().sum()
    }

    #[cfg(test)]
    pub fn count_for(&self, routing_key: &str) -> u64 {
        *self.counts.get(routing_key).unwrap_or(&0)
    }
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut entries: Vec<(&String, &u64)> = self.counts.iter().collect();
        entries.sort_by_key(|(key, _)| key.to_string());
        write!(f, "total={} ", self.total())?;
        for (key, count) in entries {
            write!(f, "{key}={count} ")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_the_same_routing_key_twice_accumulates_its_count() {
        let mut stats = Stats::default();
        stats.record("v1.organization.changed");
        stats.record("v1.organization.changed");
        assert_eq!(stats.count_for("v1.organization.changed"), 2);
    }

    #[test]
    fn recording_distinct_routing_keys_tracks_them_separately() {
        let mut stats = Stats::default();
        stats.record("v1.organization.changed");
        stats.record("v1.asset.changed.building");
        assert_eq!(stats.count_for("v1.organization.changed"), 1);
        assert_eq!(stats.count_for("v1.asset.changed.building"), 1);
    }

    #[test]
    fn total_sums_every_routing_key_count() {
        let mut stats = Stats::default();
        stats.record("v1.organization.changed");
        stats.record("v1.asset.changed.building");
        stats.record("v1.asset.changed.building");
        assert_eq!(stats.total(), 3);
    }

    #[test]
    fn count_for_an_unseen_routing_key_is_zero() {
        let stats = Stats::default();
        assert_eq!(stats.count_for("v1.user.changed"), 0);
    }
}
