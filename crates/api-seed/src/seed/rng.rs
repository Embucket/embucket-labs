use rand::{SeedableRng, rngs::StdRng};
use fake::Dummy;
use std::cell::RefCell;

pub const SEED_FOR_RANDOMIZER: u64 = 1024;

thread_local! {
    static RNG: RefCell<StdRng> = RefCell::new(StdRng::seed_from_u64(SEED_FOR_RANDOMIZER));
}

/// Faker that uses a thread-local seeded RNG
pub trait GlobalFaker: Dummy<StdRng> {
    fn fake_global() -> Self where Self: Sized {
        RNG.with(|rng| Self::dummy(&mut *rng.borrow_mut()))
    }
}

pub fn init_rng(seed: u64) {
    RNG.with(|r| *r.borrow_mut() = StdRng::seed_from_u64(seed));
}

// Blanket impl for all `Dummy<StdRng>` types
impl<T: Dummy<StdRng>> GlobalFaker for T {}
