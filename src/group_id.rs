#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct GroupId(usize);

impl GroupId {
    pub fn as_usize(&self) -> usize {
        self.0
    }
}

impl From<usize> for GroupId {
    fn from(num: usize) -> Self {
        if num == 0 {
            panic!("Group ID 0 is not valid");
        }
        Self(num)
    }
}

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_group_id() {
        let gid = GroupId::from(1);
        assert_eq!(gid.as_usize(), 1);
        assert_eq!(gid.to_string(), "1");
    }

    #[test]
    fn test_group_id_zero() {
        assert!(std::panic::catch_unwind(|| GroupId::from(0)).is_err());
    }
}
