#![cfg_attr(not(test), no_std)]
#![warn(missing_docs)]

//! MQTT v3.1.1 と v5.0 に対応する I/O 非依存な MQTT ライブラリ。
//!
//! このクレートはパケットのエンコード／デコードと接続状態の管理のみを提供する。
//! ネットワーク I/O は呼び出し側が行う。

extern crate alloc;

pub mod codec;
pub mod decoder;
pub mod error;
pub mod state;
pub(crate) mod topic_filter;

pub mod v311;
pub mod v5;
