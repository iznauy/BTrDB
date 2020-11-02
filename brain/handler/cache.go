package handler

import (
	"github.com/iznauy/BTrDB/brain"
	"github.com/iznauy/BTrDB/brain/event"
	"github.com/iznauy/BTrDB/brain/tool"
)

type CacheNoticeEventHandler struct{}

func NewCacheNoticeEventHandler() EventHandler {
	return &CacheNoticeEventHandler{}
}

func (CacheNoticeEventHandler) Process(e *event.Event) bool {
	cacheHit, _ := tool.GetUint64FromMap(e.Params, "cache_hit")
	cacheMiss, _ := tool.GetUint64FromMap(e.Params, "cache_miss")
	cacheSize, _ := tool.GetUint64FromMap(e.Params, "cache_size")
	leafCount, _ := tool.GetUint64FromMap(e.Params, "leaf_count")
	NonLeafCount, _ := tool.GetUint64FromMap(e.Params, "non_leaf_count")

	now := e.Time
	systemStats := brain.B.SystemStats
	systemStats.CacheMutex.Lock()
	defer systemStats.CacheMutex.Unlock()

	cache := systemStats.Cache
	cache.RecentCacheHit = cacheHit - cache.CacheHit
	cache.RecentCacheMiss = cacheMiss - cache.CacheMiss
	cache.CacheHit = cacheMiss
	cache.CacheMiss = cacheMiss
	cache.CacheSize = cacheSize
	cache.LeafCount = leafCount
	cache.NonLeafCount = NonLeafCount
	cache.LatestUpdateTime = now
	return true
}
