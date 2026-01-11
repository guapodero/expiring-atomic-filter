//! A transparent wrapper for [atomic-cuckoo-filter](https://crates.io/crates/atomic-cuckoo-filter)
//! providing a time-based expiration mechanism using a circular buffer.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::SystemTime;

use atomic_cuckoo_filter::{CuckooFilter, CuckooFilterBuilder, CuckooFilterBuilderError};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

pub use atomic_cuckoo_filter::{Error, Lock, LockKind};

/// A serializable approximate membership query filter supporting lock-free concurrency and time-based expiration.
#[derive(Debug, Builder, Serialize, Deserialize)]
#[builder(build_fn(private, name = "base_build", validate = "Self::validate"))]
pub struct ExpiringAtomicFilter<H = DefaultHasher>
where
    H: Hasher + Default,
{
    /// Maximum number of elements the filter can store.
    /// Divided evenly over each of the `expiration_period` intervals.
    #[builder(default = "1048576")]
    capacity: usize,

    /// Size of fingerprints in bits (must be 4, 8, 16, or 32).
    /// Default is 32 to minimize the effect of false positive `contains` checks
    /// when removing items.
    #[builder(default = "32")]
    fingerprint_size: usize,

    /// Number of fingerprints per bucket.
    #[builder(default = "4")]
    bucket_size: usize,

    /// Maximum number of evictions to try before giving up.
    #[builder(default = "500")]
    max_evictions: usize,

    /// Number of seconds each item is expected to remain in the filter. Items may remain up to
    /// `ttl + expiration_period`, but no longer than that. Must be a multiple of `expiration_period`.
    #[builder(default = "86400")]
    pub ttl: u64,

    /// Maximum number of seconds between expiration events.
    #[builder(default = "3600")]
    pub expiration_period: u64,

    /// Unix timestamp of when the filter was created.
    /// Needed to determine which slot in the buffer is selected for insert, lock, and expire operations.
    #[builder(setter(skip))]
    pub created: u64,

    /// A circular buffer of filters. At any given moment, one slot is being written to
    /// and one slot is waiting to be cleared by an externally-triggered expiration process.
    #[builder(setter(skip))]
    // otherwise serde_derive will add `H: Serialize` and `H: Deserialize`
    #[serde(bound(serialize = "H: Hasher + Default", deserialize = "H: Hasher + Default"))]
    slots: Vec<CuckooFilter<H>>,
}

impl<H> ExpiringAtomicFilter<H>
where
    H: Hasher + Default,
{
    /// Insert an item into the filter.
    ///
    /// Returns Ok(()) if the item was inserted, or Error::NotEnoughSpace if the filter is full.
    ///
    /// <https://docs.rs/atomic-cuckoo-filter/*/atomic_cuckoo_filter/struct.CuckooFilter.html#method.insert>
    pub fn insert<T: ?Sized + Hash>(&self, item: &T) -> Result<(), Error> {
        let write_slot = self.write_slot(Self::now_timestamp());
        self.slots[write_slot].insert(item)
    }

    /// Check if an item is in the filter and insert it if is not present (atomically).
    ///
    /// Returns Ok(true) if the item was inserted, Ok(false) if it was already present,
    /// or Error::NotEnoughSpace if the filter is full.
    ///
    /// <https://docs.rs/atomic-cuckoo-filter/*/atomic_cuckoo_filter/struct.CuckooFilter.html#method.insert_unique>
    pub fn insert_unique<T: ?Sized + Hash>(&self, item: &T) -> Result<bool, Error> {
        self.insert_unique_as_of(item, Self::now_timestamp())
    }

    #[inline(always)]
    fn insert_unique_as_of<T: ?Sized + Hash>(&self, item: &T, now: u64) -> Result<bool, Error> {
        let write_slot = self.write_slot(now);
        self.slots[write_slot].insert_unique(item)
    }

    /// Counts the number of occurrences of an item in the filter.
    ///
    /// <https://docs.rs/atomic-cuckoo-filter/*/atomic_cuckoo_filter/struct.CuckooFilter.html#method.count>
    pub fn count<T: ?Sized + Hash>(&self, item: &T) -> usize {
        self.slots.iter().map(|f| f.count(item)).sum()
    }

    /// Attempts to remove an item from the filter.
    ///
    /// Returns true if the item was successfully removed, or false if it was not found.
    ///
    /// <https://docs.rs/atomic-cuckoo-filter/*/atomic_cuckoo_filter/struct.CuckooFilter.html#method.remove>
    pub fn remove<T: ?Sized + Hash>(&self, item: &T) -> bool {
        for filter in &self.slots {
            // Removing a non-existent item can corrupt the filter. Although `contains`
            // can produce false positives, this risk mitigated by configuring
            // the default fingerprint size as 32.
            if filter.contains(item) && filter.remove(item) {
                return true;
            }
        }
        false
    }

    /// Check if an item is in the filter.
    ///
    /// Returns true if the item is possibly in the filter (may have false positives),
    /// false if it is definitely not in the filter.
    ///
    /// <https://docs.rs/atomic-cuckoo-filter/*/atomic_cuckoo_filter/struct.CuckooFilter.html#method.contains>
    pub fn contains<T: ?Sized + Hash>(&self, item: &T) -> bool {
        for filter in &self.slots {
            if filter.contains(item) {
                return true;
            }
        }
        false
    }

    /// Get the number of elements in the filter.
    pub fn len(&self) -> usize {
        self.slots.iter().map(|f| f.len()).sum()
    }

    /// Check if the filter is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the capacity of the filter.
    pub fn capacity(&self) -> usize {
        let slot_capacity = self.slots[0].capacity();
        // count all slots, except the fully expired one
        slot_capacity * (self.slots.len() - 1)
    }

    /// Clear the filter, removing all elements.
    pub fn clear(&self) {
        for filter in &self.slots {
            filter.clear();
        }
    }

    /// Acquires a lock on the filter, if necessary.
    ///
    /// Returns Some(Lock) if a lock is needed, or None if no locking is required.
    ///
    /// <https://docs.rs/atomic-cuckoo-filter/*/atomic_cuckoo_filter/struct.CuckooFilter.html#method.lock>
    pub fn lock(&self, kind: LockKind) -> Option<Lock<'_>> {
        let write_slot = self.write_slot(Self::now_timestamp());
        self.slots[write_slot].lock(kind)
    }

    /// Returns the number of items that were removed.
    pub fn expire(&self) -> usize {
        self.expire_as_of(Self::now_timestamp())
    }

    /// Returns the number of items that were removed.
    #[inline]
    pub fn expire_as_of(&self, now: u64) -> usize {
        let expire_slot = (1 + self.write_slot(now)) % self.slots.len();
        let filter = &self.slots[expire_slot];
        let item_count = filter.len();

        if item_count > 0 {
            filter.clear();
        }

        item_count
    }

    #[inline(always)]
    fn write_slot(&self, now: u64) -> usize {
        let slot_count = self.slots.len() as u64;
        let ttl_segment_duration = self.ttl / (slot_count - 2);
        let buffer_duration = ttl_segment_duration * slot_count;
        let now_buffer_time = (now - self.created) % buffer_duration;

        let mut write_slot_start = now_buffer_time.next_multiple_of(ttl_segment_duration);
        if !now_buffer_time.is_multiple_of(ttl_segment_duration) {
            write_slot_start -= ttl_segment_duration;
        }

        (write_slot_start / ttl_segment_duration) as usize
    }

    #[inline(always)]
    fn now_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("epoch should be earlier than now")
            .as_secs()
    }
}

impl ExpiringAtomicFilter<DefaultHasher> {
    /// Create a new ExpiringAtomicFilterBuilder with default settings
    pub fn builder() -> ExpiringAtomicFilterBuilder<DefaultHasher> {
        ExpiringAtomicFilterBuilder::default()
    }

    /// Create a new ExpiringAtomicFilter with default settings
    pub fn new() -> ExpiringAtomicFilter<DefaultHasher> {
        Self::builder().build().unwrap()
    }

    /// Create a new ExpiringAtomicFilter with the specified capacity
    pub fn with_capacity(capacity: usize) -> ExpiringAtomicFilter<DefaultHasher> {
        Self::builder().capacity(capacity).build().unwrap()
    }
}

impl Default for ExpiringAtomicFilter<DefaultHasher> {
    /// Create a new CuckooFilter with default settings
    fn default() -> Self {
        Self::new()
    }
}

impl<H> ExpiringAtomicFilterBuilder<H>
where
    H: Hasher + Default + Clone,
{
    fn validate(&self) -> Result<(), String> {
        if let (Some(ttl), Some(expiration_period)) = (self.ttl, self.expiration_period)
            && !ttl.is_multiple_of(expiration_period)
        {
            return Err("ttl must be a multiple of expiration_period".into());
        }
        Ok(())
    }

    /// Build an ExpiringAtomicFilter with the specified configuration.
    pub fn build(&self) -> Result<ExpiringAtomicFilter<H>, ExpiringAtomicFilterBuilderError> {
        let mut filter = self.base_build()?;

        filter.created = ExpiringAtomicFilter::<H>::now_timestamp();

        // Reserve two additional slots for partially expired and fully expired items.
        let slot_count = 2 + (filter.ttl / filter.expiration_period) as usize;
        let unexpired_slot_capacity = filter.capacity / (slot_count - 2);

        let mut slots = Vec::with_capacity(slot_count);
        for _ in 0..slot_count {
            let filter = CuckooFilterBuilder::default()
                .capacity(unexpired_slot_capacity)
                .fingerprint_size(filter.fingerprint_size)
                .bucket_size(filter.bucket_size)
                .max_evictions(filter.max_evictions)
                .build()
                .map_err(ExpiringAtomicFilterBuilderError::from)?;
            slots.push(filter);
        }
        filter.slots = slots;

        Ok(filter)
    }
}

impl From<CuckooFilterBuilderError> for ExpiringAtomicFilterBuilderError {
    fn from(value: CuckooFilterBuilderError) -> Self {
        match value {
            CuckooFilterBuilderError::ValidationError(mut description) => {
                if description == "capacity must be greater than zero" {
                    description = "capacity must be at least ttl / expiration_period".into();
                }
                Self::ValidationError(description)
            }
            CuckooFilterBuilderError::UninitializedField(field_name) => {
                Self::UninitializedField(field_name)
            }
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_slot() {
        let now = ExpiringAtomicFilter::<DefaultHasher>::now_timestamp();

        let filter = ExpiringAtomicFilter::builder()
            .capacity(2)
            .ttl(hs(25))
            .expiration_period(hs(12) + ms(30))
            .build()
            .unwrap();

        let cases = [
            (now + hs(12) + ms(29) + 59, 0),
            (now + hs(12) + ms(30), 1),
            (now + hs(24) + ms(59) + 59, 1),
            (now + hs(25), 2),
            (now + hs(37) + ms(29) + 59, 2),
            (now + hs(37) + ms(30), 3),
            (now + hs(49) + ms(59) + 59, 3),
            (now + hs(50), 0),
        ];

        for (now_input, slot_num) in cases {
            assert_eq!(filter.write_slot(now_input), slot_num);
        }
    }

    #[test]
    fn test_expire_as_of() {
        let now = ExpiringAtomicFilter::<DefaultHasher>::now_timestamp();

        let filter = ExpiringAtomicFilterBuilder::<ahash::AHasher>::default()
            .capacity(50)
            .ttl(hs(25))
            .expiration_period(ms(30))
            .build()
            .unwrap();

        // do not expire an item until the TTL has elapsed
        assert_eq!(
            filter.insert_unique_as_of("item1", now + ms(29) + 59),
            Ok(true)
        );
        assert_eq!(
            filter.expire_as_of(now + hs(25) + ms(29) + 59),
            0,
            "item1 at max age"
        );
        assert!(filter.contains("item1"));
        assert_eq!(
            filter.expire_as_of(now + hs(25) + ms(30)),
            1,
            "item1 expires after TTL"
        );
        assert!(!filter.contains("item1"));

        // do not expire an item older than TTL + expiration period
        assert_eq!(
            filter.insert_unique_as_of("item2", now + hs(24) + ms(59) + 59),
            Ok(true)
        );
        assert_eq!(
            filter.expire_as_of(now + hs(50) + ms(30)),
            0,
            "too late to expire item2"
        );
        assert!(filter.contains("item2"));
        assert_eq!(
            filter.expire_as_of(now + hs(50) + ms(29) + 59),
            1,
            "item2 expired at last possible time"
        );
        assert!(!filter.contains("item2"));
    }

    fn hs(i: u64) -> u64 {
        i * 3600
    }

    fn ms(i: u64) -> u64 {
        i * 60
    }
}
