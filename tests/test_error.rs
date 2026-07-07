use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};

#[test]
fn decode_error_display() {
    // 各バリアントの Display 実装がパニックしないことを確認する。
    let errors = [
        format!("{}", DecodeError::InsufficientData),
        format!("{}", DecodeError::MalformedPacket),
        format!("{}", DecodeError::InvalidPacketType),
        format!("{}", DecodeError::UnexpectedPacket { packet_type: 0x10 }),
        format!("{}", DecodeError::InvalidPacketFlags),
        format!("{}", DecodeError::InvalidUtf8),
        format!(
            "{}",
            DecodeError::PacketTooLarge {
                size: 100,
                limit: 50
            }
        ),
    ];
    for msg in &errors {
        assert!(!msg.is_empty());
    }

    // UnexpectedPacket の Display は種別を 16 進 2 桁で含む。
    assert_eq!(
        format!("{}", DecodeError::UnexpectedPacket { packet_type: 0x10 }),
        "unexpected packet type 0x10 in client-receive direction"
    );

    // InsufficientData の比較。
    assert_eq!(DecodeError::InsufficientData, DecodeError::InsufficientData);
    assert_ne!(DecodeError::InsufficientData, DecodeError::MalformedPacket);
    // MalformedPacket の比較。
    assert_eq!(DecodeError::MalformedPacket, DecodeError::MalformedPacket);
    // InvalidPacketType の比較。
    assert_eq!(
        DecodeError::InvalidPacketType,
        DecodeError::InvalidPacketType
    );
    // UnexpectedPacket の比較。
    assert_eq!(
        DecodeError::UnexpectedPacket { packet_type: 0x10 },
        DecodeError::UnexpectedPacket { packet_type: 0x10 }
    );
    assert_ne!(
        DecodeError::UnexpectedPacket { packet_type: 0x10 },
        DecodeError::UnexpectedPacket { packet_type: 0x80 }
    );
    assert_ne!(
        DecodeError::UnexpectedPacket { packet_type: 0x10 },
        DecodeError::InvalidPacketType
    );
    // InvalidPacketFlags の比較。
    assert_eq!(
        DecodeError::InvalidPacketFlags,
        DecodeError::InvalidPacketFlags
    );
    // InvalidUtf8 の比較。
    assert_eq!(DecodeError::InvalidUtf8, DecodeError::InvalidUtf8);
    // PacketTooLarge の比較。
    assert_eq!(
        DecodeError::PacketTooLarge {
            size: 100,
            limit: 50
        },
        DecodeError::PacketTooLarge {
            size: 100,
            limit: 50
        }
    );
    assert_ne!(
        DecodeError::PacketTooLarge {
            size: 100,
            limit: 50
        },
        DecodeError::PacketTooLarge {
            size: 200,
            limit: 50
        }
    );
}

#[test]
fn encode_error_display() {
    // 各バリアントの Display 実装がパニックしないことを確認する。
    let errors = [
        format!("{}", EncodeError::BufferTooSmall),
        format!(
            "{}",
            EncodeError::PacketTooLarge {
                size: 100,
                limit: 50
            }
        ),
        format!(
            "{}",
            EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptyTopicName,
            }
        ),
    ];
    for msg in &errors {
        assert!(!msg.is_empty());
    }

    // BufferTooSmall の比較。
    assert_eq!(EncodeError::BufferTooSmall, EncodeError::BufferTooSmall);
    assert_ne!(
        EncodeError::BufferTooSmall,
        EncodeError::PacketTooLarge {
            size: 100,
            limit: 50
        }
    );
    // PacketTooLarge の比較。
    assert_eq!(
        EncodeError::PacketTooLarge {
            size: 100,
            limit: 50
        },
        EncodeError::PacketTooLarge {
            size: 100,
            limit: 50
        }
    );
    assert_ne!(
        EncodeError::PacketTooLarge {
            size: 100,
            limit: 50
        },
        EncodeError::PacketTooLarge {
            size: 200,
            limit: 50
        }
    );
    // InvalidField の比較（reason が同じ場合）。
    assert_eq!(
        EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyTopicName,
        },
        EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyTopicName,
        }
    );
    // InvalidField の比較（reason が異なる場合）。
    assert_ne!(
        EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyTopicName,
        },
        EncodeError::InvalidField {
            reason: EncodeInvalidField::WildcardInTopicName,
        }
    );
    // InvalidField と他バリアントの比較。
    assert_ne!(
        EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyTopicName,
        },
        EncodeError::BufferTooSmall
    );

    // Display 文言は `invalid field: <short reason>` 形式である。
    assert_eq!(
        format!(
            "{}",
            EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptyTopicName,
            }
        ),
        "invalid field: empty topic name"
    );
}

#[test]
fn encode_invalid_field_display() {
    // EncodeInvalidField の Display は各バリアントで異なる短い英語表現を持つ。
    let cases: &[(EncodeInvalidField, &str)] = &[
        (EncodeInvalidField::EmptyTopicName, "empty topic name"),
        (
            EncodeInvalidField::WildcardInTopicName,
            "wildcard in topic name",
        ),
        (
            EncodeInvalidField::MissingPacketId,
            "missing packet identifier",
        ),
        (
            EncodeInvalidField::UnexpectedPacketId,
            "unexpected packet identifier",
        ),
        (EncodeInvalidField::ZeroPacketId, "zero packet identifier"),
        (EncodeInvalidField::DupWithQos0, "dup with qos 0"),
        (
            EncodeInvalidField::EmptySubscriptions,
            "empty subscriptions",
        ),
        (EncodeInvalidField::EmptyTopicFilters, "empty topic filters"),
        (EncodeInvalidField::EmptyReasonCodes, "empty reason codes"),
        (EncodeInvalidField::EmptyReturnCodes, "empty return codes"),
        (
            EncodeInvalidField::InvalidTopicFilter,
            "invalid topic filter",
        ),
        (
            EncodeInvalidField::SharedSubscriptionNoLocal,
            "shared subscription with no local",
        ),
        (
            EncodeInvalidField::PropertyValidationFailed,
            "property validation failed",
        ),
        (
            EncodeInvalidField::DuplicatePropertyIdentifier,
            "duplicate property identifier",
        ),
        (
            EncodeInvalidField::InvalidPropertyValue,
            "invalid property value",
        ),
        (
            EncodeInvalidField::InvalidPayloadUtf8,
            "invalid payload utf-8",
        ),
        (EncodeInvalidField::InvalidReasonCode, "invalid reason code"),
        (
            EncodeInvalidField::SessionPresentWithNonSuccess,
            "session present with non-success",
        ),
        (EncodeInvalidField::NullInUtf8String, "null in utf-8 string"),
        (
            EncodeInvalidField::PasswordWithoutUsername,
            "password without username",
        ),
        (
            EncodeInvalidField::EmptyClientIdWithoutCleanSession,
            "empty client id without clean session",
        ),
    ];
    for &(reason, expected) in cases {
        assert_eq!(format!("{reason}"), expected);
    }

    // EmptyTopicName と WildcardInTopicName は Topic Name の異なる違反として区別できる。
    assert_ne!(
        EncodeInvalidField::EmptyTopicName,
        EncodeInvalidField::WildcardInTopicName
    );
    // ZeroPacketId / MissingPacketId / UnexpectedPacketId は互いに区別できる。
    assert_ne!(
        EncodeInvalidField::ZeroPacketId,
        EncodeInvalidField::MissingPacketId
    );
    assert_ne!(
        EncodeInvalidField::ZeroPacketId,
        EncodeInvalidField::UnexpectedPacketId
    );
    assert_ne!(
        EncodeInvalidField::MissingPacketId,
        EncodeInvalidField::UnexpectedPacketId
    );
    // PropertyValidationFailed と DuplicatePropertyIdentifier は互いに区別できる。
    assert_ne!(
        EncodeInvalidField::PropertyValidationFailed,
        EncodeInvalidField::DuplicatePropertyIdentifier
    );
}
