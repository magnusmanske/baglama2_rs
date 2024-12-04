use anyhow::anyhow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct GroupId(usize);

impl GroupId {
    pub fn as_usize(&self) -> usize {
        self.0
    }
}

impl TryFrom<usize> for GroupId {
    type Error = anyhow::Error;

    fn try_from(num: usize) -> Result<Self, Self::Error> {
        if num == 0 {
            return Err(anyhow!("Group ID {num} is not valid"));
        }
        Ok(Self(num))
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
        let gid = GroupId::try_from(1).unwrap();
        assert_eq!(gid.as_usize(), 1);
        assert_eq!(gid.to_string(), "1");
    }

    #[test]
    fn test_group_id_zero() {
        assert!(GroupId::try_from(0).is_err());
    }
}
