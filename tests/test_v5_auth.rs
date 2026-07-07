//! MQTT v5.0 AUTH パケットの単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::auth::{Auth, AuthReasonCode};
use shiguredo_mqtt::v5::property::{Properties, Property};

#[test]
fn invalid_flags_are_rejected() {
    let buf = [0xF1, 0x00];
    assert_eq!(Auth::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn invalid_reason_code_is_rejected() {
    let buf = [0xF0, 0x01, 0x01];
    assert_eq!(Auth::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn auth_without_authentication_method_is_rejected() {
    // MQTT v5.0 §3.15.2.2.2: AUTH パケットには Authentication Method が必須。
    let buf = [0xF0, 0x02, 0x18, 0x00];
    assert_eq!(Auth::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn omitted_form_is_accepted() {
    // 省略形 [0xF0, 0x00] は MQTT v5.0 §3.15.2.1 末尾により Reason Code Success ・
    // プロパティなしとして受理される (詳細な解釈根拠は Auth::decode の実装コメント参照)。
    let (auth, consumed) = Auth::decode(&[0xF0, 0x00]).expect("省略形はデコードに成功すること");
    assert_eq!(
        auth,
        Auth {
            reason_code: AuthReasonCode::Success,
            properties: Properties::new(),
        }
    );
    assert_eq!(consumed, 2);
}

#[test]
fn intermediate_form_without_property_length_success_is_rejected() {
    // MQTT v5.0 §3.15.2.1 末尾は Reason Code と Property Length を同時に省略する形のみを
    // 許容する。Property Length フィールドを独立に省略した中間形は許容対象外で、
    // Auth::decode 内では空 Properties を組み立てた直後の validate_for_auth() が
    // MQTT v5.0 §3.15.2.2.2 の Authentication Method 必須で拒否する。
    assert_eq!(
        Auth::decode(&[0xF0, 0x01, 0x00]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn intermediate_form_without_property_length_continue_is_rejected() {
    // Reason Code 0x18 (Continue authentication) を持つ中間形も MQTT v5.0 §3.15.2.1 末尾の
    // 許容対象外で、MQTT v5.0 §3.15.2.2.2 の Authentication Method 必須で拒否される。
    assert_eq!(
        Auth::decode(&[0xF0, 0x01, 0x18]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn intermediate_form_without_property_length_reauthenticate_is_rejected() {
    // Reason Code 0x19 (Re-authenticate) は Client 送信専用のため
    // (MQTT v5.0 §3.15.2.1 Table 3-11)、中間形 [0xF0, 0x01, 0x19] は Auth::decode の
    // 方向検証で Properties::decode 呼び出し前に短絡拒否される。方向検証を
    // Properties デコードより前に置く実装順序の非回帰を担保する。
    assert_eq!(
        Auth::decode(&[0xF0, 0x01, 0x19]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn explicit_success_with_empty_properties_is_rejected() {
    // MQTT v5.0 §2.2.2.1 [MQTT-2.2.2-1] により Property Length 0 は
    // 「プロパティ集合が空である旨の明示」を意味し、集合は物理的に存在する扱いになる。
    // したがって明示形 [0xF0, 0x02, 0x00, 0x00] は MQTT v5.0 §3.15.2.2.2 の
    // Authentication Method 必須の適用対象となり拒否する。中間形と異なり
    // Properties::decode を通る分岐であることに注意。
    assert_eq!(
        Auth::decode(&[0xF0, 0x02, 0x00, 0x00]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn method_property_form_is_accepted() {
    // Authentication Method プロパティを含む正常系 AUTH は引き続き受理される
    // (MQTT v5.0 §3.15.2.2.2)。Reason Code は Success (Server → Client) を用いる。
    let (auth, consumed) =
        Auth::decode(&[0xF0, 0x08, 0x00, 0x06, 0x15, 0x00, 0x03, b'S', b'C', b'M'])
            .expect("Method 付き正常系はデコードに成功すること");
    let mut expected_properties = Properties::new();
    expected_properties.push(Property::AuthenticationMethod("SCM".to_string()));
    assert_eq!(
        auth,
        Auth {
            reason_code: AuthReasonCode::Success,
            properties: expected_properties,
        }
    );
    assert_eq!(consumed, 10);
}

#[test]
fn extra_bytes_after_auth_are_rejected() {
    // 残り長さを偽装して余計なバイトを追加する。
    let buf = [0xF0, 0x03, 0x00, 0x00, 0x00];
    assert_eq!(Auth::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn reason_code_direction_matrix_is_enforced() {
    // MQTT v5.0 §3.15.2.1 Table 3-11 の「Sent by」列に基づく方向別検証。
    // 0x00 (Success) は Server のみ、0x18 (Continue authentication) は Client or Server、
    // 0x19 (Re-authenticate) は Client のみ。
    // AUTH には Authentication Method プロパティが必須のため、常に付与する。
    fn auth_with_method(reason_code: AuthReasonCode) -> Auth {
        let mut properties = Properties::new();
        properties.push(
            shiguredo_mqtt::v5::property::Property::AuthenticationMethod("SCRAM".to_string()),
        );
        Auth {
            reason_code,
            properties,
        }
    }
    // (reason_code, Client が送れるか, Server が送れるか)
    let cases: &[(AuthReasonCode, bool, bool)] = &[
        (AuthReasonCode::Success, false, true),
        (AuthReasonCode::ContinueAuthentication, true, true),
        (AuthReasonCode::ReAuthenticate, true, false),
    ];
    for &(reason_code, client_can_send, server_can_send) in cases {
        let auth = auth_with_method(reason_code);
        let mut buf = [0u8; 32];
        let encoded = auth.encode(&mut buf);
        if client_can_send {
            let len = encoded.expect("クライアントが送れる Reason Code はエンコードできること");
            let decoded = Auth::decode(&buf[..len]);
            if server_can_send {
                assert_eq!(
                    decoded,
                    Ok((auth.clone(), len)),
                    "サーバーが送れる Reason Code はデコードできること: {reason_code:?}"
                );
            } else {
                assert_eq!(
                    decoded,
                    Err(DecodeError::MalformedPacket),
                    "サーバーが送れない Reason Code はデコードを拒否すること: {reason_code:?}"
                );
            }
        } else {
            assert_eq!(
                encoded,
                Err(EncodeError::InvalidField {
                    reason: EncodeInvalidField::InvalidReasonCode,
                }),
                "クライアントが送れない Reason Code はエンコードを拒否すること: {reason_code:?}"
            );
            // decode 方向は Authentication Method プロパティ付きの手組みバイト列で確認する。
            let raw = [
                0xF0,
                0x06,
                reason_code.as_u8(),
                0x04,
                0x15,
                0x00,
                0x01,
                b'A',
            ];
            let (packet, _) =
                Auth::decode(&raw).expect("サーバーが送れる Reason Code はデコードできること");
            assert_eq!(packet.reason_code, reason_code);
        }
    }
}

#[test]
fn invalid_property_is_rejected_on_encode() {
    // AUTH で許可されていない Topic Alias (0x23) を含むプロパティを追加する。
    // Authentication Method もないため、validate_for_auth の両方の検証で拒否される。
    // Reason Code は方向検証を通過させるため Client 送信可能な 0x18 を使う。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::TopicAlias(1));
    let auth = Auth {
        reason_code: AuthReasonCode::ContinueAuthentication,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        auth.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn authentication_method_accessor_returns_property_value() {
    // Authentication Method プロパティを含む AUTH からアクセサで取り出せる
    // (MQTT v5.0 §3.15.2.2.2)。
    let mut properties = Properties::new();
    properties.push(Property::AuthenticationMethod("SCRAM-SHA-256".to_string()));
    let auth = Auth {
        reason_code: AuthReasonCode::ContinueAuthentication,
        properties,
    };
    assert_eq!(auth.authentication_method(), Some("SCRAM-SHA-256"));
}

#[test]
fn authentication_method_accessor_returns_none_when_absent() {
    // codec 層は AUTH に Authentication Method を必須とするが、アクセサは
    // プロパティが無い場合に None を返す (呼び出し側の防御的判定用)。
    let auth = Auth {
        reason_code: AuthReasonCode::Success,
        properties: Properties::new(),
    };
    assert_eq!(auth.authentication_method(), None);
}

#[test]
fn duplicate_auth_property_identifiers_are_rejected_on_encode() {
    // AUTH のプロパティに Reason String (0x1F) が重複している場合、エンコードを拒否する。
    // Reason Code は方向検証を通過させるため Client 送信可能な 0x18 を使う。
    let mut properties = Properties::new();
    properties
        .push(shiguredo_mqtt::v5::property::Property::AuthenticationMethod("method".to_string()));
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "a".to_string(),
    ));
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "b".to_string(),
    ));
    let auth = Auth {
        reason_code: AuthReasonCode::ContinueAuthentication,
        properties,
    };
    let mut buf = [0u8; 64];
    assert_eq!(
        auth.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}

#[test]
fn debug_masks_authentication_data_in_auth_packet() {
    // AUTH パケットの Debug 経由でも AuthenticationData が平文で出ないこと。
    let mut properties = Properties::new();
    properties.push(Property::AuthenticationMethod("SCRAM-SHA-256".to_string()));
    properties.push(Property::AuthenticationData(vec![0x01, 0x02, 0x03]));
    let auth = Auth {
        reason_code: AuthReasonCode::ContinueAuthentication,
        properties,
    };
    let debug = format!("{:?}", auth);
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("0x01"));
    assert!(!debug.contains("[1, 2, 3]"));
}
