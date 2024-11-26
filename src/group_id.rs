#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct GroupId(usize);

impl GroupId {
    pub fn is_valid(&self) -> bool {
        self.0 > 0
    }

    pub fn as_usize(&self) -> usize {
        self.0
    }
}

impl From<usize> for GroupId {
    fn from(num: usize) -> Self {
        Self(num)
    }
}

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
