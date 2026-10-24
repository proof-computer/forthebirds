//! Extracted verbatim from Planetarium src/star_names.rs at
//! commit b9723db8e5b9fcd6fe9fa5e49fa3d9ee0384b384.
//!
//! Deterministic, presentation-only names for peers, never identity keys.
//!
//! Always show the peer id beside its name. Collisions are expected among the
//! 2,112 combinations; callers supply the ordering key for collision ranks.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

pub const GREEK: [&str; 24] = [
    "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa",
    "Lambda", "Mu", "Nu", "Xi", "Omicron", "Pi", "Rho", "Sigma", "Tau", "Upsilon", "Phi", "Chi",
    "Psi", "Omega",
];

/// IAU genitives in alphabetical order of the constellation's nominative name.
pub const CONSTELLATIONS: [&str; 88] = [
    "Andromedae",
    "Antliae",
    "Apodis",
    "Aquarii",
    "Aquilae",
    "Arae",
    "Arietis",
    "Aurigae",
    "Boötis",
    "Caeli",
    "Camelopardalis",
    "Cancri",
    "Canum Venaticorum",
    "Canis Majoris",
    "Canis Minoris",
    "Capricorni",
    "Carinae",
    "Cassiopeiae",
    "Centauri",
    "Cephei",
    "Ceti",
    "Chamaeleontis",
    "Circini",
    "Columbae",
    "Comae Berenices",
    "Coronae Australis",
    "Coronae Borealis",
    "Corvi",
    "Crateris",
    "Crucis",
    "Cygni",
    "Delphini",
    "Doradus",
    "Draconis",
    "Equulei",
    "Eridani",
    "Fornacis",
    "Geminorum",
    "Gruis",
    "Herculis",
    "Horologii",
    "Hydrae",
    "Hydri",
    "Indi",
    "Lacertae",
    "Leonis",
    "Leonis Minoris",
    "Leporis",
    "Librae",
    "Lupi",
    "Lyncis",
    "Lyrae",
    "Mensae",
    "Microscopii",
    "Monocerotis",
    "Muscae",
    "Normae",
    "Octantis",
    "Ophiuchi",
    "Orionis",
    "Pavonis",
    "Pegasi",
    "Persei",
    "Phoenicis",
    "Pictoris",
    "Piscium",
    "Piscis Austrini",
    "Puppis",
    "Pyxidis",
    "Reticuli",
    "Sagittae",
    "Sagittarii",
    "Scorpii",
    "Sculptoris",
    "Scuti",
    "Serpentis",
    "Sextantis",
    "Tauri",
    "Telescopii",
    "Trianguli",
    "Trianguli Australis",
    "Tucanae",
    "Ursae Majoris",
    "Ursae Minoris",
    "Velorum",
    "Virginis",
    "Volantis",
    "Vulpeculae",
];

const NAME_COUNT: usize = GREEK.len() * CONSTELLATIONS.len();

/// Hash the full peer id's UTF-8 bytes into the fixed name catalogue.
pub fn star_index(peer_id: &str) -> u16 {
    let digest = Sha256::digest(peer_id.as_bytes());
    let prefix = u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 has eight prefix bytes"),
    );
    (prefix % NAME_COUNT as u64) as u16
}

fn name_at(index: usize) -> String {
    format!(
        "{} {}",
        GREEK[index % GREEK.len()],
        CONSTELLATIONS[index / GREEK.len()]
    )
}

/// Return the peer's base name, before any collision numeral.
pub fn star_name(peer_id: &str) -> String {
    name_at(star_index(peer_id) as usize)
}

/// Append the one-based collision rank only when it is at least two.
pub fn numbered(base: &str, rank: usize) -> String {
    if rank >= 2 {
        format!("{base} {rank}")
    } else {
        base.to_owned()
    }
}

/// Rank members of one base name by `(key, id)`, starting at one.
///
/// Repeated ids keep their smallest key. The caller chooses what the key means.
pub fn collision_ranks(members: &[(String, u64)]) -> BTreeMap<String, usize> {
    let mut keys = BTreeMap::<&str, u64>::new();
    for (id, key) in members {
        keys.entry(id.as_str())
            .and_modify(|smallest| *smallest = (*smallest).min(*key))
            .or_insert(*key);
    }
    let mut ordered: Vec<_> = keys.into_iter().map(|(id, key)| (key, id)).collect();
    ordered.sort_unstable();
    ordered
        .into_iter()
        .enumerate()
        .map(|(index, (_, id))| (id.to_owned(), index + 1))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn catalogue_contains_2112_distinct_names() {
        assert_eq!(GREEK.len(), 24);
        assert_eq!(CONSTELLATIONS.len(), 88);
        assert_eq!(GREEK.into_iter().collect::<BTreeSet<_>>().len(), 24);
        assert_eq!(
            CONSTELLATIONS.into_iter().collect::<BTreeSet<_>>().len(),
            88
        );
        let names: BTreeSet<_> = (0..2112).map(name_at).collect();
        assert_eq!(names.len(), 2112);
    }

    #[test]
    fn catalogue_boundaries_keep_greek_letters_as_the_inner_index() {
        assert_eq!(name_at(0), "Alpha Andromedae");
        assert_eq!(name_at(23), "Omega Andromedae");
        assert_eq!(name_at(24), "Alpha Antliae");
        assert_eq!(name_at(2111), "Omega Vulpeculae");
        assert_eq!(name_at(8 * 24), "Alpha Boötis");
    }

    #[test]
    fn full_peer_ids_match_fixed_vectors() {
        for (id, index, name) in [
            ("peer-158aa7bd235be5659419762aa3eecc24", 1850, "Gamma Tauri"),
            (
                "peer-41dbda9a1a4b287ee097ca501ab2178a",
                1394,
                "Gamma Ophiuchi",
            ),
            (
                "peer-0d1b558d9e6a93663992de8faa7bc733",
                1767,
                "Pi Sculptoris",
            ),
            (
                "peer-6eeb03a31e154526f2d9a7e284e5ce93",
                1534,
                "Psi Phoenicis",
            ),
            ("peer-b60bbdf8756a4686c265882adda8214d", 112, "Rho Aquilae"),
            (
                "peer-7dc47a47eae3853d650abc2d7612c935",
                600,
                "Alpha Coronae Australis",
            ),
            (
                "peer-f7d37a0371d81064db50d47f10f92a13",
                1920,
                "Alpha Trianguli Australis",
            ),
            ("peer-a", 962, "Gamma Horologii"),
            (
                "peer-b5c3ef6d34250c172c3e441742a7391a",
                1027,
                "Upsilon Hydri",
            ),
            (
                "peer-f33c2e3fe602ccb3546e86222766ff11",
                1027,
                "Upsilon Hydri",
            ),
        ] {
            assert_eq!(star_index(id), index, "{id}");
            assert_eq!(star_name(id), name, "{id}");
        }
    }

    #[test]
    fn real_collision_is_ranked_by_key_then_full_id() {
        let first = "peer-b5c3ef6d34250c172c3e441742a7391a";
        let second = "peer-f33c2e3fe602ccb3546e86222766ff11";
        let tied = collision_ranks(&[(second.into(), 500), (first.into(), 500)]);
        assert_eq!(tied[first], 1);
        assert_eq!(tied[second], 2);
        let different = collision_ranks(&[(first.into(), 200), (second.into(), 100)]);
        assert_eq!(different[second], 1);
        assert_eq!(different[first], 2);
        assert_eq!(numbered("Upsilon Hydri", tied[first]), "Upsilon Hydri");
        assert_eq!(numbered("Upsilon Hydri", tied[second]), "Upsilon Hydri 2");
    }

    #[test]
    fn duplicate_ids_keep_the_smallest_key_including_zero() {
        let mut members = vec![
            ("peer-z".into(), 80),
            ("peer-a".into(), 10),
            ("peer-z".into(), 0),
            ("peer-a".into(), 20),
            ("peer-z".into(), 90),
        ];
        let expected = BTreeMap::from([("peer-z".into(), 1), ("peer-a".into(), 2)]);
        assert_eq!(collision_ranks(&members), expected);
        members.reverse();
        assert_eq!(collision_ranks(&members), expected);
    }

    #[test]
    fn empty_and_single_member_groups_need_no_numeral() {
        assert!(collision_ranks(&[]).is_empty());
        let ranks = collision_ranks(&[("peer-a".into(), u64::MAX)]);
        assert_eq!(ranks, BTreeMap::from([("peer-a".into(), 1)]));
        assert_eq!(
            numbered("Gamma Horologii", ranks["peer-a"]),
            "Gamma Horologii"
        );
        assert_eq!(numbered("Gamma Horologii", 24), "Gamma Horologii 24");
    }
}
