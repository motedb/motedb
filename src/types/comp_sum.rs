/// Neumaier compensated summation for float SUM/AVG aggregates.
///
/// Differential testing vs SQLite (8909-check randomized run) caught naive
/// `+=` accumulation diverging in the 4th decimal place on ~50-value
/// aggregates. SQLite compensates its summation internally; this matches
/// that semantics and is strictly more accurate than naive accumulation.
#[derive(Clone, Copy, Debug, Default)]
pub struct CompSum {
    sum: f64,
    comp: f64,
}

impl CompSum {
    #[inline]
    pub fn add(&mut self, x: f64) {
        let t = self.sum + x;
        if self.sum.abs() >= x.abs() {
            self.comp += (self.sum - t) + x;
        } else {
            self.comp += (x - t) + self.sum;
        }
        self.sum = t;
    }

    /// Seeded constructor (e.g. promoting an integer running sum).
    #[inline]
    pub fn from_value(v: f64) -> Self {
        Self { sum: v, comp: 0.0 }
    }

    #[inline]
    pub fn total(&self) -> f64 {
        self.sum + self.comp
    }
}
