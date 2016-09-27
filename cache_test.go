package freecache

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	DemoString = `{"data_source":0,"driver_meters":{"3090049679":{"order_id":3090049679,"cap_seat":1,"is_carpool_succ":false,"pre_total_price":780,"travel_end":0,"dist_fallback_method":0,"realtime_pricing_count":0,"begin_charge_lng":"0","begin_charge_lat":"0","real_normal_distance":18949.80554898524,"real_time":2083.5,"normal_distance":18949.80554898524,"time":2083.5,"night_distance":0,"empty_distance":0,"low_speed_time_normal":0,"low_speed_time_peak":0,"dynamic_price":0,"dynamic_times":80,"discount":0,"fixed_price":0,"cap_discount_max_fee":0,"combo_use_distance":0,"combo_remain_distance":0,"combo_use_time":0,"combo_remain_time":0,"package_price":0,"highway_fee":0,"bridge_fee":0,"park_fee":0,"other_fee":0,"punish_fee":0,"forward_fee":0,"tip_fee":0,"bonus_fee":0,"LocationsNum":0,"NetLocationsNum":0,"IsLostLocationsFor5Min":false,"IsAdjacentLocMoreThan1kmAnd3Min":false,"CreateScene":2,"DelteGap":false},"3090649300":{"order_id":3090649300,"cap_seat":1,"is_carpool_succ":false,"pre_total_price":202,"travel_end":0,"dist_fallback_method":0,"realtime_pricing_count":0,"begin_charge_lng":"0","begin_charge_lat":"0","real_normal_distance":4651.611318297932,"real_time":521.5,"normal_distance":4651.611318297932,"time":521.5,"night_distance":0,"empty_distance":0,"low_speed_time_normal":0,"low_speed_time_peak":0,"dynamic_price":0,"dynamic_times":0,"discount":0,"fixed_price":0,"cap_discount_max_fee":0,"combo_use_distance":0,"combo_remain_distance":0,"combo_use_time":0,"combo_remain_time":0,"package_price":0,"highway_fee":0,"bridge_fee":0,"park_fee":0,"other_fee":0,"punish_fee":0,"forward_fee":0,"tip_fee":0,"bonus_fee":0,"LocationsNum":0,"NetLocationsNum":0,"IsLostLocationsFor5Min":false,"IsAdjacentLocMoreThan1kmAnd3Min":false,"CreateScene":2,"DelteGap":false}},"latest_locations":{"3090049679":{"x":116.434183,"y":40.02189,"timestamp":1468993769,"locType":1},"3090649300":{"x":116.328917,"y":40.05729,"timestamp":1468992700,"locType":1}},"orders":{"3090049679":{"order_id":3090049679,"driver_id":565076229622303,"driver_phone":"17310132686","passenger_id":2947409184770,"passenger_phone":"13520574347","passenger_count":1,"travel_id":7678452161427373667,"schema_id":5,"combo_type":4,"combo_id":0,"strategy_token":"3774f082a15593a560024c3e0e517755","car_id":0,"area":1,"type":0,"extra_type":3262177,"driver_type":1,"product_id":3,"tip":0,"token":"","product_token":"","is_sep":1,"limit_fee":0,"cap_price":54.7,"dynamic_price":800,"delay_time_start":"","begin_charge_time":"2016-07-20 13:06:01","finish_time":"","driver_display_price":0,"channel":806,"pre_total_fee":78,"pangu":"[]","is_airport":0,"bonus":0,"bouns":0,"airport":0,"strive_car_level":"","start_dest_distance":"0","departure_time":"","starting_lng":"0","starting_lat":"0","dest_lng":"0","dest_lat":"0","order_status":4,"district":"010","abstract_district":"","begin_charge_lng":"0","begin_charge_lat":"0"},"3090649300":{"order_id":3090649300,"driver_id":565076229622303,"driver_phone":"17310132686","passenger_id":25305376,"passenger_phone":"15901099625","passenger_count":1,"travel_id":7678452161427373667,"schema_id":5,"combo_type":4,"combo_id":0,"strategy_token":"3774f082a15593a560024c3e0e517755","car_id":0,"area":1,"type":0,"extra_type":3262181,"driver_type":1,"product_id":3,"tip":0,"token":"","product_token":"","is_sep":1,"limit_fee":0,"cap_price":12.6,"dynamic_price":0,"delay_time_start":"","begin_charge_time":"2016-07-20 13:14:20","finish_time":"","driver_display_price":0,"channel":780,"pre_total_fee":20.2,"pangu":"[]","is_airport":0,"bonus":0,"bouns":0,"airport":0,"strive_car_level":"","start_dest_distance":"0","departure_time":"","starting_lng":"0","starting_lat":"0","dest_lng":"0","dest_lat":"0","order_status":5,"district":"010","abstract_district":"","begin_charge_lng":"0","begin_charge_lat":"0"}},"passenger_meters":{"3090049679":{"order_id":3090049679,"cap_seat":1,"is_carpool_succ":false,"pre_total_price":780,"travel_end":0,"dist_fallback_method":0,"realtime_pricing_count":0,"begin_charge_lng":"0","begin_charge_lat":"0","real_normal_distance":23603.634756770414,"real_time":3596,"normal_distance":23603.634756770414,"time":3596,"night_distance":0,"empty_distance":11603.634756770414,"low_speed_time_normal":0,"low_speed_time_peak":0,"dynamic_price":0,"dynamic_times":80,"discount":0,"fixed_price":547,"cap_discount_max_fee":0,"combo_use_distance":0,"combo_remain_distance":0,"combo_use_time":0,"combo_remain_time":0,"package_price":0,"highway_fee":0,"bridge_fee":0,"park_fee":0,"other_fee":0,"punish_fee":0,"forward_fee":0,"tip_fee":0,"bonus_fee":0,"LocationsNum":0,"NetLocationsNum":0,"IsLostLocationsFor5Min":false,"IsAdjacentLocMoreThan1kmAnd3Min":false,"CreateScene":2,"DelteGap":false},"3090649300":{"order_id":3090649300,"cap_seat":1,"is_carpool_succ":false,"pre_total_price":202,"travel_end":0,"dist_fallback_method":0,"realtime_pricing_count":0,"begin_charge_lng":"0","begin_charge_lat":"0","real_normal_distance":9302.967024333864,"real_time":1040,"normal_distance":9302.967024333864,"time":1040,"night_distance":0,"empty_distance":0,"low_speed_time_normal":0,"low_speed_time_peak":0,"dynamic_price":0,"dynamic_times":0,"discount":0,"fixed_price":126,"cap_discount_max_fee":0,"combo_use_distance":0,"combo_remain_distance":0,"combo_use_time":0,"combo_remain_time":0,"package_price":0,"highway_fee":0,"bridge_fee":0,"park_fee":0,"other_fee":0,"punish_fee":0,"forward_fee":0,"tip_fee":0,"bonus_fee":0,"LocationsNum":0,"NetLocationsNum":0,"IsLostLocationsFor5Min":false,"IsAdjacentLocMoreThan1kmAnd3Min":false,"CreateScene":2,"DelteGap":false}}}`

)

func TestFreeCache(t *testing.T) {
	cache := NewCache(1024*1024*50)
	if cache.HitRate() != 0 {
		t.Error("initial hit rate should be zero")
	}
	if cache.AverageAccessTime() != 0 {
		t.Error("initial average access time should be zero")
	}
	key := []byte("abcd")
	val := []byte("efghijkl")
	err := cache.Set(key, val, 0)
	if err != nil {
		t.Error("err should be nil")
	}
	value, err := cache.Get(key)
	if err != nil || !bytes.Equal(value, val) {
		t.Error("value not equal")
	}
	affected := cache.Del(key)
	if !affected {
		t.Error("del should return affected true")
	}
	value, err = cache.Get(key)
	if err != ErrNotFound {
		t.Error("error should be ErrNotFound after being deleted")
	}
	affected = cache.Del(key)
	if affected {
		t.Error("del should not return affected true")
	}

	cache.Clear()
	n := 4000
	for i := 0; i < n; i++ {
		keyStr := fmt.Sprintf("key%v", i)
		valStr := DemoString + strings.Repeat(keyStr, 10)
		if i == 0 {
			err = cache.Set([]byte(keyStr), []byte(valStr), 0)
		} else {
			err = cache.Set([]byte(keyStr), []byte(valStr), 5)
		}

		if err != nil {
			t.Error(err)
		}
	}
	t.Logf("hit rate is %v, evacuates %v, entries %v, average time %v, expire count %v, total count %v, cur %v\n",
		cache.HitRate(), cache.EvacuateCount(), cache.EntryCount(), cache.AverageAccessTime(), cache.ExpiredCount(), cache.TotalCount(), time.Now().Unix())



	//for i := 1; i < n; i += 2 {
	//	keyStr := fmt.Sprintf("key%v", i)
	//	cache.Get([]byte(keyStr))
	//}
	//
	//for i := 1; i < n; i += 8 {
	//	keyStr := fmt.Sprintf("key%v", i)
	//	cache.Del([]byte(keyStr))
	//}



	for j := 1; j <= 10; j++ {
		time.Sleep(time.Second*10)
		key := fmt.Sprintf("key%v", 0)
		_, err := cache.Get([]byte(key))
		if err != nil {
			t.Error(err)
		}
		base := j * n
		errCount := 0
		for i := 0; i < n; i += 1 {
			keyStr := fmt.Sprintf("key%v", base+i)
			valStr := DemoString + strings.Repeat(keyStr, 10)
			err = cache.Set([]byte(keyStr), []byte(valStr), 5)
			if err != nil {
				//t.Error(err)
				errCount += 1
			}
		}


		for i := 0; i < n; i += 1 {
			keyStr := fmt.Sprintf("key%v", base+i)
			expectedValStr := DemoString + strings.Repeat(keyStr, 10)
			value, err = cache.Get([]byte(keyStr))
			if err == nil {
				if string(value) != expectedValStr {
					t.Errorf("value is %v, expected %v", string(value), expectedValStr)
				}
			} else {
				t.Error(err)
			}
		}


		t.Logf("hit rate is %v, evacuates %v, entries %v, average time %v, expire count %v, total count %v, cur %v, errCount %v\n",
			cache.HitRate(), cache.EvacuateCount(), cache.EntryCount(), cache.AverageAccessTime(), cache.ExpiredCount(), cache.TotalCount(), time.Now().Unix(), errCount)
	}


	time.Sleep(time.Second*10)
	for i := 1; i < n; i += 1 {
		keyStr := fmt.Sprintf("key%v", i)
		expectedValStr := strings.Repeat(keyStr, 10)
		_ = expectedValStr
		value, err = cache.Get([]byte(keyStr))
		if err == nil {
			//if string(value) != expectedValStr {
			//	t.Errorf("value is %v, expected %v", string(value), expectedValStr)
			//}
		}
	}

	t.Logf("hit rate is %v, evacuates %v, entries %v, average time %v, expire count %v, total count %v, cur %v\n",
		cache.HitRate(), cache.EvacuateCount(), cache.EntryCount(), cache.AverageAccessTime(), cache.ExpiredCount(), cache.TotalCount(), time.Now().Unix())

	cache.ResetStatistics()
	t.Logf("hit rate is %v, evacuates %v, entries %v, average time %v, expire count %v, total count %v, cur %v\n",
		cache.HitRate(), cache.EvacuateCount(), cache.EntryCount(), cache.AverageAccessTime(), cache.ExpiredCount(), cache.TotalCount(), time.Now().Unix())
}

func TestOverwrite(t *testing.T) {
	cache := NewCache(1024)
	key := []byte("abcd")
	var val []byte
	cache.Set(key, val, 0)
	val = []byte("efgh")
	cache.Set(key, val, 0)
	val = append(val, 'i')
	cache.Set(key, val, 0)
	if count := cache.OverwriteCount(); count != 0 {
		t.Error("overwrite count is", count, "expected ", 0)
	}
	res, _ := cache.Get(key)
	if string(res) != string(val) {
		t.Error(string(res))
	}
	val = append(val, 'j')
	cache.Set(key, val, 0)
	res, _ = cache.Get(key)
	if string(res) != string(val) {
		t.Error(string(res), "aaa")
	}
	val = append(val, 'k')
	cache.Set(key, val, 0)
	res, _ = cache.Get(key)
	if string(res) != "efghijk" {
		t.Error(string(res))
	}
	val = append(val, 'l')
	cache.Set(key, val, 0)
	res, _ = cache.Get(key)
	if string(res) != "efghijkl" {
		t.Error(string(res))
	}
	val = append(val, 'm')
	cache.Set(key, val, 0)
	if count := cache.OverwriteCount(); count != 3 {
		t.Error("overwrite count is", count, "expected ", 3)
	}

}

func TestExpire(t *testing.T) {
	cache := NewCache(1024)
	key := []byte("abcd")
	val := []byte("efgh")
	err := cache.Set(key, val, 1)
	if err != nil {
		t.Error("err should be nil")
	}
	time.Sleep(time.Second)
	val, err = cache.Get(key)
	if err == nil {
		t.Fatal("key should be expired", string(val))
	}

	cache.ResetStatistics()
	if cache.ExpiredCount() != 0 {
		t.Error("expired count should be zero.")
	}
}

func TestLargeEntry(t *testing.T) {
	cacheSize := 512 * 1024
	cache := NewCache(cacheSize)
	key := make([]byte, 65536)
	val := []byte("efgh")
	err := cache.Set(key, val, 0)
	if err != ErrLargeKey {
		t.Error("large key should return ErrLargeKey")
	}
	val, err = cache.Get(key)
	if val != nil {
		t.Error("value should be nil when get a big key")
	}
	key = []byte("abcd")
	maxValLen := cacheSize/1024 - ENTRY_HDR_SIZE - len(key)
	val = make([]byte, maxValLen+1)
	err = cache.Set(key, val, 0)
	if err != ErrLargeEntry {
		t.Error("err should be ErrLargeEntry", err)
	}
	val = make([]byte, maxValLen-2)
	err = cache.Set(key, val, 0)
	if err != nil {
		t.Error(err)
	}
	val = append(val, 0)
	err = cache.Set(key, val, 0)
	if err != nil {
		t.Error(err)
	}
	val = append(val, 0)
	err = cache.Set(key, val, 0)
	if err != nil {
		t.Error(err)
	}
	if cache.OverwriteCount() != 1 {
		t.Error("over write count should be one.")
	}
	val = append(val, 0)
	err = cache.Set(key, val, 0)
	if err != ErrLargeEntry {
		t.Error("err should be ErrLargeEntry", err)
	}

	cache.ResetStatistics()
	if cache.OverwriteCount() != 0 {
		t.Error("over write count should be zero.")
	}
}

func TestInt64Key(t *testing.T) {
	cache := NewCache(1024)
	err := cache.SetInt(1, []byte("abc"), 0)
	if err != nil {
		t.Error("err should be nil")
	}
	err = cache.SetInt(2, []byte("cde"), 0)
	if err != nil {
		t.Error("err should be nil")
	}
	val, err := cache.GetInt(1)
	if err != nil {
		t.Error("err should be nil")
	}
	if !bytes.Equal(val, []byte("abc")) {
		t.Error("value not equal")
	}
	affected := cache.DelInt(1)
	if !affected {
		t.Error("del should return affected true")
	}
	_, err = cache.GetInt(1)
	if err != ErrNotFound {
		t.Error("error should be ErrNotFound after being deleted")
	}
}

func BenchmarkCacheSet(b *testing.B) {
	cache := NewCache(256 * 1024 * 1024)
	var key [8]byte
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[:], uint64(i))
		cache.Set(key[:], make([]byte, 8), 0)
	}
}

func BenchmarkMapSet(b *testing.B) {
	m := make(map[string][]byte)
	var key [8]byte
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[:], uint64(i))
		m[string(key[:])] = make([]byte, 8)
	}
}

func BenchmarkCacheGet(b *testing.B) {
	b.StopTimer()
	cache := NewCache(256 * 1024 * 1024)
	var key [8]byte
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[:], uint64(i))
		cache.Set(key[:], make([]byte, 8), 0)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[:], uint64(i))
		cache.Get(key[:])
	}
}

func BenchmarkMapGet(b *testing.B) {
	b.StopTimer()
	m := make(map[string][]byte)
	var key [8]byte
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[:], uint64(i))
		m[string(key[:])] = make([]byte, 8)
	}
	b.StartTimer()
	var hitCount int64
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[:], uint64(i))
		if m[string(key[:])] != nil {
			hitCount++
		}
	}
}

func BenchmarkHashFunc(b *testing.B) {
	key := make([]byte, 8)
	rand.Read(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hashFunc(key)
	}
}
