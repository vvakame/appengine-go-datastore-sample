package sample

import (
	"appengine"
	"appengine/aetest"
	"appengine/datastore"
	pb "appengine_internal/datastore"
	"code.google.com/p/goprotobuf/proto"
	"reflect"
	"strings"
	"testing"
	"time"
)

// from https://cloud.google.com/appengine/docs/go/datastore/entities

type Foo struct {
	Integer     int
	Float       float32
	Boolean     bool
	StringShort string
	StringLong  string `datastore:",noindex"`
	ByteShort   datastore.ByteString
	ByteLong    []byte
	Time        time.Time
	GeoPoint    appengine.GeoPoint
	Blobstore   appengine.BlobKey
	Key         *datastore.Key
}

func TestExistingKeyIsNotUpdated(t *testing.T) {
	opt := &aetest.Options{StronglyConsistentDatastore: true}
	c, err := aetest.NewContext(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	foo := &Foo{}

	key := datastore.NewIncompleteKey(c, "Foo", nil)
	newKey, err := datastore.Put(c, key, foo)

	if err != nil {
		t.Fatal(err)
	}

	if key.Incomplete() != true {
		t.Fatal()
	}

	if newKey.Incomplete() != false {
		t.Fatal()
	}

	if foo.Time.IsZero() != true {
		t.Fatal()
	}
}

type Bar struct {
	Keys []*datastore.Key
}

func TestBatchGetRelations(t *testing.T) {
	opt := &aetest.Options{StronglyConsistentDatastore: true}
	c, err := aetest.NewContext(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// make children
	foos := make([]Foo, 3)
	foos[0].Integer = 10
	foos[1].Integer = 11
	foos[2].Integer = 12
	keys := make([]*datastore.Key, 0)
	for _, _ = range foos {
		keys = append(keys, datastore.NewIncompleteKey(c, "Foo", nil))
	}
	keys, err = datastore.PutMulti(c, keys, foos)
	if err != nil {
		t.Fatal(err)
	}

	// make entity
	key := datastore.NewIncompleteKey(c, "Bar", nil)
	if err != nil {
		t.Fatal(err)
	}
	bar := &Bar{keys}
	key, err = datastore.Put(c, key, bar)
	if err != nil {
		t.Fatal(err)
	}

	// Get entity
	bar = &Bar{}
	err = datastore.Get(c, key, bar)
	if err != nil {
		t.Fatal(err)
	}

	// Get children
	foos = make([]Foo, len(bar.Keys))
	err = datastore.GetMulti(c, bar.Keys, foos)
	if err != nil {
		t.Fatal(err)
	}

	// assert
	if len(foos) != 3 {
		t.Fatal()
	}
	if foos[0].Integer != 10 {
		t.Fatal()
	}
	if foos[1].Integer != 11 {
		t.Fatal()
	}
	if foos[2].Integer != 12 {
		t.Fatal()
	}

	// Query by key
	q := datastore.NewQuery("Bar").Filter("Keys =", bar.Keys[1])

	// count
	cnt, err := q.Count(c)
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Fatal()
	}

	// getAll
	var bars []Bar
	keys, err = q.GetAll(c, &bars)
	if err != nil {
		t.Fatal(err)
	}

	// assert
	if len(bars) != 1 {
		t.Fatal()
	}
	if len(bars[0].Keys) != 3 {
		t.Fatal()
	}
}

type Buzz struct {
	Foo1 Foo
	Foo2 Foo
	Foo3 Foo `datastore:"foofoo3"`
}

func TestHasStructInStruct(t *testing.T) {
	opt := &aetest.Options{StronglyConsistentDatastore: true}
	c, err := aetest.NewContext(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// make children
	key := datastore.NewIncompleteKey(c, "Buzz", nil)
	if err != nil {
		t.Fatal(err)
	}
	buzz := &Buzz{Foo{Integer: 1}, Foo{Integer: 2}, Foo{Integer: 3}}
	key, err = datastore.Put(c, key, buzz)
	if err != nil {
		t.Fatal(err)
	}

	// check properties structure
	res, err := GetProto(c, key)
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Entity) != 1 {
		t.Fatal()
	}

	if len(res.Entity[0].GetEntity().Property) < 3 {
		t.Fatal()
	}
	propertyNames := make([]string, 0)
	for _, p := range res.Entity[0].GetEntity().Property {
		propertyNames = append(propertyNames, *p.Name)
	}
	if reflect.DeepEqual(propertyNames, strings.Split("Foo1.Integer Foo1.Float Foo1.Boolean Foo1.StringShort Foo1.ByteShort Foo1.Time Foo1.GeoPoint Foo1.Blobstore Foo1.Key Foo2.Integer Foo2.Float Foo2.Boolean Foo2.StringShort Foo2.ByteShort Foo2.Time Foo2.GeoPoint Foo2.Blobstore Foo2.Key foofoo3.Integer foofoo3.Float foofoo3.Boolean foofoo3.StringShort foofoo3.ByteShort foofoo3.Time foofoo3.GeoPoint foofoo3.Blobstore foofoo3.Key", " ")) == false {
		t.Fatal(propertyNames)
	}
}

func TestQueryForStructInStruct(t *testing.T) {
	opt := &aetest.Options{StronglyConsistentDatastore: true}
	c, err := aetest.NewContext(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	key := datastore.NewIncompleteKey(c, "Buzz", nil)
	if err != nil {
		t.Fatal(err)
	}
	buzz := &Buzz{Foo{Integer: 1}, Foo{Integer: 2}, Foo{Integer: 3}}
	key, err = datastore.Put(c, key, buzz)
	if err != nil {
		t.Fatal(err)
	}

	// query
	q := datastore.NewQuery("Buzz").Filter("foofoo3.Integer =", 3)
	cnt, err := q.Count(c)
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Fatal()
	}
}

type Slice struct {
	Integers     []int
	Floats       []float32
	Booleans     []bool
	StringShorts []string
	StringLongs  []string `datastore:",noindex"`
	ByteShorts   []datastore.ByteString
	ByteLongs    [][]byte
	Times        []time.Time
	GeoPoints    []appengine.GeoPoint
	Blobstores   []appengine.BlobKey
	Key          []*datastore.Key
}

func TestPutSliceStruct(t *testing.T) {
	opt := &aetest.Options{StronglyConsistentDatastore: true}
	c, err := aetest.NewContext(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	key := datastore.NewIncompleteKey(c, "Slice", nil)
	if err != nil {
		t.Fatal(err)
	}
	sl := &Slice{
		Integers:     []int{1, 2},
		Floats:       []float32{1, 2},
		Booleans:     []bool{true, false},
		StringShorts: []string{"a", "b"},
		StringLongs:  []string{"a", "b"},
		ByteShorts:   []datastore.ByteString{[]byte{}, []byte{}},
		ByteLongs:    [][]byte{{1}, {2}},
		Times:        []time.Time{time.Now(), time.Now()},
		GeoPoints:    []appengine.GeoPoint{appengine.GeoPoint{1, 2}, appengine.GeoPoint{3, 4}},
		Blobstores:   []appengine.BlobKey{"", ""},
		Key:          []*datastore.Key{key, key},
	}
	key, err = datastore.Put(c, key, sl)
	if err != nil {
		t.Fatal(err)
	}

	// check properties structure
	res, err := GetProto(c, key)
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Entity) != 1 {
		t.Fatal()
	}

	if len(res.Entity[0].GetEntity().Property) < 3 {
		t.Fatal(res)
	}
	propertyNames := make([]string, 0)
	for _, p := range res.Entity[0].GetEntity().Property {
		propertyNames = append(propertyNames, *p.Name)
	}
	if reflect.DeepEqual(propertyNames, strings.Split("Integers Integers Floats Floats Booleans Booleans StringShorts StringShorts ByteShorts ByteShorts Times Times GeoPoints GeoPoints Blobstores Blobstores Key Key", " ")) == false {
		t.Fatal(propertyNames)
	}
}

type Map struct {
	Dict map[string]string
}

func TestPutWithMapValue(t *testing.T) {
	opt := &aetest.Options{StronglyConsistentDatastore: true}
	c, err := aetest.NewContext(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	key := datastore.NewIncompleteKey(c, "Map", nil)
	m := &Map{}
	key, err = datastore.Put(c, key, m)
	if err.Error() != "datastore: unsupported struct field type: map[string]string" {
		t.Fatal(err)
	}
}

type Sub struct {
	Rev int
}

type Main struct {
	Sub
	Name string
	Age  int
}

func TestPutEmbedStruct(t *testing.T) {
	opt := &aetest.Options{StronglyConsistentDatastore: true}
	c, err := aetest.NewContext(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	key := datastore.NewIncompleteKey(c, "Main", nil)
	m := &Main{Sub{1}, "vv", 30}
	key, err = datastore.Put(c, key, m)
	if err != nil {
		t.Fatal(err)
	}

	// check properties structure
	res, err := GetProto(c, key)
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Entity) != 1 {
		t.Fatal()
	}

	if len(res.Entity[0].GetEntity().Property) < 3 {
		t.Fatal()
	}
	propertyNames := make([]string, 0)
	for _, p := range res.Entity[0].GetEntity().Property {
		propertyNames = append(propertyNames, *p.Name)
	}
	if reflect.DeepEqual(propertyNames, strings.Split("Rev Name Age", " ")) == false {
		t.Fatal(propertyNames)
	}
}

type Before1 struct {
	A int
	B int
}

type After1 struct {
	A int
	C int
}

func TestSchemaHasDiff(t *testing.T) {
	opt := &aetest.Options{StronglyConsistentDatastore: true}
	c, err := aetest.NewContext(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	key := datastore.NewIncompleteKey(c, "Foo", nil)
	b := &Before1{1, 2}
	key, err = datastore.Put(c, key, b)
	if err != nil {
		t.Fatal(err)
	}

	a := &After1{}
	err = datastore.Get(c, key, a)
	if err.Error() != `datastore: cannot load field "B" into a "sample.After1": no such struct field` {
		t.Fatal(err)
	}
}

type Before2 struct {
	A int
	B int
}

type After2 struct {
	A           int
	C           int
	DeprecatedB int `datastore:"B,noindex"`
}

func TestSchemaHasDiffWithHint(t *testing.T) {
	opt := &aetest.Options{StronglyConsistentDatastore: true}
	c, err := aetest.NewContext(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	key := datastore.NewIncompleteKey(c, "Foo", nil)
	b := &Before2{1, 2}
	key, err = datastore.Put(c, key, b)
	if err != nil {
		t.Fatal(err)
	}

	a := &After2{}
	err = datastore.Get(c, key, a)
	if err != nil {
		t.Fatal(err)
	}

	if a.A != 1 {
		t.Fatal()
	}
	if a.C != 0 {
		t.Fatal()
	}
	if a.DeprecatedB != 2 {
		t.Fatal()
	}
}

func GetProto(c appengine.Context, key *datastore.Key) (*pb.GetResponse, error) {
	return GetProtoMulti(c, []*datastore.Key{key})
}

func GetProtoMulti(c appengine.Context, keys []*datastore.Key) (*pb.GetResponse, error) {
	pbKeys := make([]*pb.Reference, len(keys))
	for idx, key := range keys {
		n := 0
		for i := key; i != nil; i = i.Parent() {
			n++
		}
		e := make([]*pb.Path_Element, n)
		for i := key; i != nil; i = i.Parent() {
			n--
			kind := i.Kind()
			e[n] = &pb.Path_Element{
				Type: &kind,
			}
			if key.StringID() != "" {
				name := key.StringID()
				e[n].Name = &name
			} else if key.IntID() != 0 {
				id := key.IntID()
				e[n].Id = &id
			}
		}

		namespace := key.Namespace()
		pbKeys[idx] = &pb.Reference{
			App:       proto.String(c.FullyQualifiedAppID()),
			NameSpace: &namespace,
			Path: &pb.Path{
				Element: e,
			},
		}
	}

	req := &pb.GetRequest{
		Key: pbKeys,
	}
	res := &pb.GetResponse{}
	if err := c.Call("datastore_v3", "Get", req, res, nil); err != nil {
		return nil, err
	}

	return res, nil
}
