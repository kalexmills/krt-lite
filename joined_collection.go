package krtlite

// Joined represents a joined value where Left and Right both share identical keys. Used in collections created using
// Join.
type Joined[L, R any] struct {
	Type  JoinType
	Left  *L
	Right *R
	key   string
}

func (j Joined[L, R]) Key() string {
	return j.key
}

// JoinType specifies different rules for handling nil entries in Join.
type JoinType string

const (
	// LeftJoin ensures all entries from the left collection are present, even if no matching right entries exist.
	LeftJoin JoinType = "left"
	// RightJoin ensures all entries from the right collection are present, even if no matching left entries exist.
	RightJoin JoinType = "right"
	// InnerJoin ensures all entries have non-nil Left and Right entries.
	InnerJoin JoinType = "inner"
)

// Join is used to join two collections -- named left and right -- which share a key space. Items which share a key are
// combined into a single Joined entry in the resulting collection.
//
// JoinType is used to control how items without a match are handled. Left will always be non-nil for items in LeftJoin
// collections, while Right will always be non-nil for items in RightJoin collections. Both Left and Right will be
// non-nil for items in InnerJoin collections.
//
// A given Joined entry will always be updated anytime their corresponding items are updated upstream. The event type
// reflected in the joined collection does not match the type of the upstream event in every case. LeftJoin collections
// only receive Add and Delete events for their Left entries; Add and Delete events for their Right entries are
// reflected as Updates. The symmetric statement is true for RightJoin collections. InnerJoin collections act like
// LeftJoin collections in this respect.
func Join[L, R any](left Collection[L], right Collection[R], joinType JoinType, opts ...CollectionOption) Collection[Joined[L, R]] {
	switch joinType {
	case LeftJoin:
		return Map(left, func(ktx Context, l L) *Joined[L, R] {
			k := GetKey(l)
			r := FetchOne(ktx, right, k)
			return &Joined[L, R]{
				key:   k,
				Type:  LeftJoin,
				Left:  &l,
				Right: r,
			}
		}, opts...)

	case RightJoin:
		return Map(right, func(ktx Context, r R) *Joined[L, R] {
			k := GetKey(r)
			l := FetchOne(ktx, left, k)
			return &Joined[L, R]{
				key:   k,
				Type:  RightJoin,
				Left:  l,
				Right: &r,
			}
		}, opts...)

	case InnerJoin:
		// At first glance, it seems that by implementing InnerJoin as a call to Map(left,...) we may miss some updates from
		// the right collection. But the only updates we miss will be those who don't have a matching key in left, so this
		// doesn't matter.
		return Map(left, func(ktx Context, l L) *Joined[L, R] {
			k := GetKey(l)
			r := FetchOne(ktx, right, k)
			if r == nil {
				return nil
			}
			return &Joined[L, R]{
				key:   k,
				Type:  InnerJoin,
				Left:  &l,
				Right: r,
			}
		})

	default:
		panic("unsupported join type: " + joinType)
	}
}
