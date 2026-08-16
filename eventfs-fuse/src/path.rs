//! 用户可见路径的严格解析。

use std::fmt;

use es_core::validate_aggregate_identifier;

/// 单次挂载内可见的逻辑节点。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Node {
    Root,
    BusinessSpace(String),
    AggregateType {
        business_space: String,
        aggregate_type: String,
    },
    Events {
        business_space: String,
        aggregate_type: String,
    },
    States {
        business_space: String,
        aggregate_type: String,
    },
    State {
        business_space: String,
        aggregate_type: String,
        aggregate_id: String,
    },
    Groups {
        business_space: String,
        aggregate_type: String,
    },
    Group {
        business_space: String,
        aggregate_type: String,
        group_name: String,
    },
    Consumer {
        business_space: String,
        aggregate_type: String,
        group_name: String,
        consumer_id: String,
    },
}

impl Node {
    /// 解析绝对路径。
    ///
    /// # 参数
    /// `path` 必须以 `/` 开头，且不能包含空段、`.`、`..` 或尾随 `/`。
    ///
    /// # 返回
    /// 返回文件系统逻辑节点。
    ///
    /// # 错误
    /// 层级、扩展名或标识符不符合公开契约时返回 [`PathError`]。
    pub fn parse(path: &str) -> Result<Self, PathError> {
        if path == "/" {
            return Ok(Self::Root);
        }
        if !path.starts_with('/') || path.ends_with('/') || path.contains("//") {
            return Err(PathError::InvalidShape);
        }
        let mut node = Self::Root;
        for segment in path[1..].split('/') {
            node = node.child(segment)?;
        }
        Ok(node)
    }

    /// 解析当前目录下的单个子项。
    ///
    /// # 参数
    /// `name` 是未经转义的单个 UTF-8 文件名。
    ///
    /// # 返回
    /// 返回对应子节点。
    ///
    /// # 错误
    /// 当前节点不是目录、名称非法或固定名称不匹配时返回 [`PathError`]。
    pub fn child(&self, name: &str) -> Result<Self, PathError> {
        if name.is_empty() || name == "." || name == ".." || name.contains('/') {
            return Err(PathError::InvalidName);
        }
        match self {
            Self::Root => {
                identifier("business_space", name)?;
                Ok(Self::BusinessSpace(name.into()))
            }
            Self::BusinessSpace(business_space) => {
                identifier("aggregate_type", name)?;
                Ok(Self::AggregateType {
                    business_space: business_space.clone(),
                    aggregate_type: name.into(),
                })
            }
            Self::AggregateType {
                business_space,
                aggregate_type,
            } => match name {
                "events.jsonl" => Ok(Self::Events {
                    business_space: business_space.clone(),
                    aggregate_type: aggregate_type.clone(),
                }),
                "states" => Ok(Self::States {
                    business_space: business_space.clone(),
                    aggregate_type: aggregate_type.clone(),
                }),
                "groups" => Ok(Self::Groups {
                    business_space: business_space.clone(),
                    aggregate_type: aggregate_type.clone(),
                }),
                _ => Err(PathError::NotFound),
            },
            Self::States {
                business_space,
                aggregate_type,
            } => {
                let aggregate_id = strip_extension(name, ".json")?;
                identifier("aggregate_id", aggregate_id)?;
                Ok(Self::State {
                    business_space: business_space.clone(),
                    aggregate_type: aggregate_type.clone(),
                    aggregate_id: aggregate_id.into(),
                })
            }
            Self::Groups {
                business_space,
                aggregate_type,
            } => {
                identifier("group_name", name)?;
                Ok(Self::Group {
                    business_space: business_space.clone(),
                    aggregate_type: aggregate_type.clone(),
                    group_name: name.into(),
                })
            }
            Self::Group {
                business_space,
                aggregate_type,
                group_name,
            } => {
                let consumer_id = strip_extension(name, ".jsonl")?;
                identifier("consumer_id", consumer_id)?;
                Ok(Self::Consumer {
                    business_space: business_space.clone(),
                    aggregate_type: aggregate_type.clone(),
                    group_name: group_name.clone(),
                    consumer_id: consumer_id.into(),
                })
            }
            Self::Events { .. } | Self::State { .. } | Self::Consumer { .. } => {
                Err(PathError::NotDirectory)
            }
        }
    }

    /// 返回节点是否为目录。
    pub fn is_directory(&self) -> bool {
        matches!(
            self,
            Self::Root
                | Self::BusinessSpace(_)
                | Self::AggregateType { .. }
                | Self::States { .. }
                | Self::Groups { .. }
                | Self::Group { .. }
        )
    }

    /// 返回节点所在聚合类型；根和业务空间没有聚合类型。
    pub fn aggregate_type(&self) -> Option<(&str, &str)> {
        match self {
            Self::AggregateType {
                business_space,
                aggregate_type,
            }
            | Self::Events {
                business_space,
                aggregate_type,
            }
            | Self::States {
                business_space,
                aggregate_type,
            }
            | Self::State {
                business_space,
                aggregate_type,
                ..
            }
            | Self::Groups {
                business_space,
                aggregate_type,
            }
            | Self::Group {
                business_space,
                aggregate_type,
                ..
            }
            | Self::Consumer {
                business_space,
                aggregate_type,
                ..
            } => Some((business_space, aggregate_type)),
            Self::Root | Self::BusinessSpace(_) => None,
        }
    }
}

fn strip_extension<'a>(name: &'a str, extension: &str) -> Result<&'a str, PathError> {
    name.strip_suffix(extension)
        .filter(|stem| !stem.is_empty())
        .ok_or(PathError::InvalidExtension)
}

fn identifier(kind: &str, value: &str) -> Result<(), PathError> {
    validate_aggregate_identifier(kind, value).map_err(|_| PathError::InvalidName)
}

/// 路径解析失败原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathError {
    InvalidShape,
    InvalidName,
    InvalidExtension,
    NotDirectory,
    NotFound,
}

impl fmt::Display for PathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for PathError {}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_all_public_paths() {
        assert_eq!(Node::parse("/").unwrap(), Node::Root);
        assert!(matches!(
            Node::parse("/orders/order/events.jsonl").unwrap(),
            Node::Events { .. }
        ));
        assert_eq!(
            Node::parse("/orders/order/states/order-1.json").unwrap(),
            Node::State {
                business_space: "orders".into(),
                aggregate_type: "order".into(),
                aggregate_id: "order-1".into(),
            }
        );
        assert_eq!(
            Node::parse("/orders/order/groups/workers/consumer-a.jsonl").unwrap(),
            Node::Consumer {
                business_space: "orders".into(),
                aggregate_type: "order".into(),
                group_name: "workers".into(),
                consumer_id: "consumer-a".into(),
            }
        );
    }

    #[test]
    fn rejects_ambiguous_or_wrong_extension_paths() {
        for path in [
            "orders/order/events.jsonl",
            "/orders//order",
            "/orders/order/",
            "/orders/order/states/a.jsonl",
            "/orders/order/groups/g/c.json",
            "/orders/order/unknown",
            "/orders/order/states/../x.json",
        ] {
            assert!(Node::parse(path).is_err(), "必须拒绝 {path}");
        }
    }

    #[test]
    fn node_kinds_report_directory_and_aggregate_type_membership() {
        let paths = [
            "/",
            "/orders",
            "/orders/order",
            "/orders/order/events.jsonl",
            "/orders/order/states",
            "/orders/order/states/order-1.json",
            "/orders/order/groups",
            "/orders/order/groups/workers",
            "/orders/order/groups/workers/consumer-a.jsonl",
        ];
        let nodes = paths
            .into_iter()
            .map(|path| Node::parse(path).unwrap())
            .collect::<Vec<_>>();
        assert!(nodes[..3].iter().all(Node::is_directory));
        assert!(!nodes[3].is_directory());
        assert!(nodes[4].is_directory());
        assert!(!nodes[5].is_directory());
        assert!(nodes[6..8].iter().all(Node::is_directory));
        assert!(!nodes[8].is_directory());
        assert!(nodes[0].aggregate_type().is_none());
        assert!(nodes[1].aggregate_type().is_none());
        for node in &nodes[2..] {
            assert_eq!(node.aggregate_type(), Some(("orders", "order")));
        }

        for leaf in [&nodes[3], &nodes[5], &nodes[8]] {
            assert_eq!(leaf.child("extra"), Err(PathError::NotDirectory));
        }
        for invalid in ["", ".", "..", "a/b", "_leading"] {
            assert!(Node::Root.child(invalid).is_err());
        }
        assert_eq!(
            Node::parse("/orders/order/states").unwrap().child(".json"),
            Err(PathError::InvalidExtension)
        );
        assert_eq!(PathError::NotFound.to_string(), "NotFound");
    }

    proptest! {
        #[test]
        fn arbitrary_utf8_path_never_panics(path in ".{0,512}") {
            let _ = Node::parse(&path);
        }
    }
}
