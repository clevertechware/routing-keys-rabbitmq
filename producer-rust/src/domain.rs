use serde::Serialize;

/// Business events published on the `platform.events` topic exchange.
/// Each variant maps to a routing key shaped as `v1.<entity>.<event>[.<discriminant>]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusinessEvent {
    OrganizationChanged,
    AssetChangedBook,
    AssetChangedVideo,
    InvoiceChanged,
    DocumentChangedContract,
    UserChanged,
}

impl BusinessEvent {
    pub const ALL: [BusinessEvent; 6] = [
        BusinessEvent::OrganizationChanged,
        BusinessEvent::AssetChangedBook,
        BusinessEvent::AssetChangedVideo,
        BusinessEvent::InvoiceChanged,
        BusinessEvent::DocumentChangedContract,
        BusinessEvent::UserChanged,
    ];

    pub fn routing_key(&self) -> &'static str {
        match self {
            BusinessEvent::OrganizationChanged => "v1.organization.changed",
            BusinessEvent::AssetChangedBook => "v1.asset.changed.book",
            BusinessEvent::AssetChangedVideo => "v1.asset.changed.video",
            BusinessEvent::InvoiceChanged => "v1.invoice.changed",
            BusinessEvent::DocumentChangedContract => "v1.document.changed.contract",
            BusinessEvent::UserChanged => "v1.user.changed",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct EventPayload {
    pub id: String,
    pub routing_key: &'static str,
    pub sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn organization_changed_has_no_discriminant() {
        assert_eq!(BusinessEvent::OrganizationChanged.routing_key(), "v1.organization.changed");
    }

    #[test]
    fn asset_changed_building_carries_its_discriminant() {
        assert_eq!(BusinessEvent::AssetChangedBook.routing_key(), "v1.asset.changed.building");
    }

    #[test]
    fn asset_changed_vehicle_carries_its_discriminant() {
        assert_eq!(BusinessEvent::AssetChangedVideo.routing_key(), "v1.asset.changed.vehicle");
    }

    #[test]
    fn every_routing_key_starts_with_the_v1_namespace() {
        for event in BusinessEvent::ALL {
            assert!(event.routing_key().starts_with("v1."));
        }
    }

    #[test]
    fn every_routing_key_is_unique() {
        let mut keys: Vec<&str> = BusinessEvent::ALL.iter().map(|e| e.routing_key()).collect();
        let original_len = keys.len();
        keys.sort_unstable();
        keys.dedup();
        assert_eq!(keys.len(), original_len);
    }

    #[test]
    fn every_routing_key_has_at_least_an_entity_and_an_event_segment() {
        for event in BusinessEvent::ALL {
            let segments: Vec<&str> = event.routing_key().split('.').collect();
            assert!(segments.len() >= 3, "unexpected shape for {}", event.routing_key());
        }
    }
}
