use std::num::NonZero;

pub type GroupId = NonZero<usize>;

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_group_id() {
        let gid = GroupId::try_from(1).unwrap();
        assert_eq!(gid.get(), 1);
        assert_eq!(gid.to_string(), "1");
    }

    #[test]
    fn test_group_id_zero() {
        assert!(GroupId::try_from(0).is_err());
    }
}
