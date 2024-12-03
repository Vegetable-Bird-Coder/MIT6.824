package shardctrler

type ConfigStateMachine interface {
	Join(servers map[int][]string) (Err, *Config)
	Leave(gids []int) (Err, *Config)
	Move(shard, gid int) (Err, *Config)
	Query(num int) (Err, *Config)
}

type MemoryConfig struct {
	configs []Config
}

func newMemoryConfig() *MemoryConfig {
	cf := new(MemoryConfig)
	cf.configs = make([]Config, 1)
	cf.configs[0].Groups = map[int][]string{}
	return cf
}

func (cf *MemoryConfig) Join(servers map[int][]string) (Err, *Config) {
	lastConfig := &cf.configs[len(cf.configs)-1]
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: deepCopy(lastConfig.Groups)}
	for gid, server := range servers {
		newConfig.Groups[gid] = append([]string(nil), server...)
	}

	gid2shards := makeGid2Shards(&newConfig)
	for {
		source := getGidWithMaxShards(gid2shards)
		target := getGidWithMinShards(gid2shards)
		if source != 0 && len(gid2shards[source])-len(gid2shards[target]) <= 1 {
			break
		}
		gid2shards[target] = append(gid2shards[target], gid2shards[source][0])
		gid2shards[source] = gid2shards[source][1:]
	}

	for gid, shards := range gid2shards {
		for _, shard := range shards {
			newConfig.Shards[shard] = gid
		}
	}

	cf.configs = append(cf.configs, newConfig)

	return OK, &cf.configs[len(cf.configs)-1]
}

func (cf *MemoryConfig) Leave(gids []int) (Err, *Config) {
	lastConfig := &cf.configs[len(cf.configs)-1]
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: deepCopy(lastConfig.Groups)}

	gid2shards := makeGid2Shards(&newConfig)
	for _, gid := range gids {
		if shards, ok := gid2shards[gid]; ok {
			gid2shards[0] = append(gid2shards[0], shards...)
			delete(gid2shards, gid)
			delete(newConfig.Groups, gid)
		}
	}

	if len(gid2shards) > 1 {
		for _, shard := range gid2shards[0] {
			target := getGidWithMinShards(gid2shards)
			gid2shards[target] = append(gid2shards[target], shard)
		}
		delete(gid2shards, 0)
	}

	for gid, shards := range gid2shards {
		for _, shard := range shards {
			newConfig.Shards[shard] = gid
		}
	}

	cf.configs = append(cf.configs, newConfig)

	return OK, &cf.configs[len(cf.configs)-1]
}

func (cf *MemoryConfig) Move(shard, gid int) (Err, *Config) {
	lastConfig := &cf.configs[len(cf.configs)-1]
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: deepCopy(lastConfig.Groups)}

	newConfig.Shards[shard] = gid
	cf.configs = append(cf.configs, newConfig)

	return OK, &cf.configs[len(cf.configs)-1]
}

func (cf *MemoryConfig) Query(num int) (Err, *Config) {
	lastConfig := &cf.configs[len(cf.configs)-1]
	if num == -1 || num > lastConfig.Num {
		return OK, lastConfig
	}
	return OK, &cf.configs[num]
}
