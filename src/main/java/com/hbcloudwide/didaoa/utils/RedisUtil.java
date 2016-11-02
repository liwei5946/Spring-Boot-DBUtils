package com.hbcloudwide.didaoa.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

/**
 * redicache 工具类
 * 
 */
@SuppressWarnings("unchecked")
@Component
public class RedisUtil {
	@SuppressWarnings("rawtypes")
	@Autowired
	private RedisTemplate redisTemplate;
	
	/**
	 * 超时时间 长时间60分钟，短时间30分钟
	 */
	public static final Long TIME_OUT_LONG = 60*60L;
	public static final Long TIME_OUT_SHORT = 30*60L;
	
	/**
	 * 获取 RedisSerializer
	 * 
	 */
	protected RedisSerializer<String> getRedisSerializer() {
		return redisTemplate.getStringSerializer();
	}
//	public RedisTemplate getRedisTemplate() {
//		return redisTemplate;
//	}
//	public void setRedisTemplate(RedisTemplate redisTemplate) {
//		this.redisTemplate = redisTemplate;
//	}
	
	/**
	 * 删除key对应的value
	 * 
	 * @param key
	 */
	public void remove(final String key) {
		if (exists(key)) {
			redisTemplate.delete(key);
		}
	}

	/**
	 * 批量删除key对应的value
	 * 
	 * @param keys
	 */
	public void remove(final String... keys) {
		for (String key : keys) {
			remove(key);
		}
	}

	/**
	 * 按照正则表达式条件，批量删除key
	 * 生产环境慎用，对CPU资源消耗极大
	 * 
	 * @param pattern
	 */
	public void removePattern(final String pattern) {
		Set<Serializable> keys = redisTemplate.keys(pattern);
		if (keys.size() > 0)
			redisTemplate.delete(keys);
	}
	
	/**
	 * 根据key删除
	 * @param key
	 * @return 返回删除成功的个数
	 */
	public Long removeCount(final String key) {
		if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<Long>() {
				public Long doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					return connection.del(keys);
				}
			});
		}
		return null;
	}

	

	/**
	 * 判断缓存中是否有对应的value
	 * 
	 * @param key
	 * @return
	 */
	public boolean exists(final String key) {
		return redisTemplate.hasKey(key);
	}
	
	/**
	 * 判断key是否存在
	 * @param key
	 * @return
	 */
	public Boolean existsKey(final String key) {
		if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<Boolean>() {
				public Boolean doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					return connection.exists(keys);
				}
			});
		}
		return false;
	}

	/**
	 * 读取缓存,根据key获取对象
	 * 
	 * @param key
	 * @return Object
	 */
	public Object get(final String key) {
		Object result = null;
		ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
		result = operations.get(key);
		return result;
	}
	
	/**
	 * 读取缓存,根据key获取字符串
	 * @param key
	 * @return String
	 */
	public String getStr(final String key) {
		if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<String>() {
				public String doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					byte[] values = connection.get(keys);
					if (values == null) {
						return null;
					}
					String value = serializer.deserialize(values);
					return value;
				}
			});
		}
		return null;
	}
	

	/**
	 * 写入缓存
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean set(final String key, Object value) {
		boolean result = false;
		try {
			ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
			operations.set(key, value);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * 设置key,写入缓存
	 */
	public Boolean set(final String key, final String value) {
		if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<Boolean>() {
				public Boolean doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					byte[] values = serializer.serialize(value);
					connection.set(keys, values);
					return true;
				}
			});
		}
		return false;
	}

	/**
	 * 写入缓存，并设置超时时间
	 * @param key
	 * @param value
	 * @param expireTimeSeconds 秒
	 * @return
	 */
	public boolean set(final String key, Object value, Long expireTimeSeconds) {
		boolean result = false;
		try {
			ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
			operations.set(key, value);
			redisTemplate.expire(key, expireTimeSeconds, TimeUnit.SECONDS);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * 对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)
	 * @param key
	 * @param offset
	 * @param value
	 * @return
	 */
	public Boolean setBit(final String key,final long offset,final boolean value) {
		if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<Boolean>() {
				public Boolean doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					connection.setBit(keys,offset,value);
					return true;
				}
			});
		}
		return false;
	}
	
	/**
	 * 对 key 所储存的字符串值，获取指定偏移量上的位(bit)
	 * @param key
	 * @param value
	 * @return
	 */
	public Boolean getBit(final String key ,final long value) {
		if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<Boolean>() {
				public Boolean doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					return connection.getBit(keys, value);
				}
			});
		}
		return false;
	}
	
	 /**
     * Sort the elements for key. 
     * @param key
     * @param params
     * @return
     */
    public List<String> sort(final String key,final SortParameters params) {
    	if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<List<String>>() {
				public List<String> doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					List<String> data = new ArrayList<>();
					List<byte[]> re = connection.sort(keys, params);
					for(byte[] by : re){
						data.add(serializer.deserialize(by));
					}
					return data;
				}
			});
		}
    	return null;
    }
	
	/**
	 * 某段时间后执行
	 * @param key
	 * @param value
	 * @return
	 */
	public Boolean expire(final String key, final long value) {
		if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<Boolean>() {
				public Boolean doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					return connection.expire(keys, value);
				}
			});
		}
		return false;
	}
	
	/**
	 * 在某个时间点失效
	 * @param key
	 * @param value
	 * @return
	 */
	public Boolean expireAt(final String key, final long value) {
		if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<Boolean>() {
				public Boolean doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					return connection.expireAt(keys, value);
				}
			});
		}
		return false;
	}
	
	/**
	 * 查询剩余时间
	 * @param key
	 * @param value
	 * @return
	 */
	public Long ttl(final String key, final long value) {
		if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<Long>() {
				public Long doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					return connection.ttl(keys);
				}
			});
		}
		return 0l;
	}
	
	/**
	 * 返回 key 所储存的值的类型
	 * @param key
	 * @return
	 */
	public DataType type(final String key) {
		if (redisTemplate != null) {
			redisTemplate.execute(new RedisCallback<DataType>() {
				public DataType doInRedis(RedisConnection connection)
						throws DataAccessException {
					RedisSerializer<String> serializer = getRedisSerializer();
					byte[] keys = serializer.serialize(key);
					return connection.type(keys);
				}
			});
		}
		return null;
	}
	
	/**
	 * 设置key的值,并返回它的旧值
	 * Set value of key and return its old value. 
	 * @param key
	 * @param value
	 * @return 旧值 如果key不存在 则返回null 
	 */
	 public byte[] getSet(final String key,final String value) {
		 if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<byte[]>() {
					public byte[] doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] values = serializer.serialize(value);
						return connection.getSet(keys, values);
					}
				});
			}
			return null;
	    }
	 
	 /**
	  * 设置key value,如果key已经存在则返回0（nx 表示 not exist）
	  * Set value for key, only if key does not exist. 
	  * @param key
	  * @param value
	  * @return 成功返回1 如果存在 和 发生异常 返回 0 
	  */
	    public Boolean setNX(final String key,final String value) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] values = serializer.serialize(value);
						return connection.setNX(keys,values);
					}
				});
			}
			return null;
	    }
	    
	    /**
	     * 设置key value并制定这个键值的有效期
	     * Set the value and expiration in seconds for key. 
	     * @param key
	     * @param seconds
	     * @param value
	     * @return 成功返回OK 失败和异常返回null 
	     */
	    public Boolean setEx(final String key,final Long seconds,final String value) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] values = serializer.serialize(value);
						connection.setEx(keys, seconds, values);
						return true;
					}
				});
			}
			return false;
	    }
	    
		/**
		 * 用 value 参数覆写(overwrite)给定 key 所储存的字符串值，从偏移量 offset 开始
		 * @param key
		 * @param offset
		 * @param value
		 * @return
		 */
		public Boolean setRange(final String key,final Long offset,final String value) {
			if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] values = serializer.serialize(value);
						connection.setRange(keys,values,offset);
						return true;
					}
				});
			}
			return false;
		}
		
		/**
		 * 返回 key 中字符串值的子字符串，字符串的截取范围由 start 和 end 两个偏移量决定
		 * @param key
		 * @param startOffset
		 * @param endOffset
		 * @return
		 */
		public byte[] getRange(final String key,final long startOffset,final long endOffset) {
			if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<byte[]>() {
					public byte[] doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.getRange(keys,startOffset,endOffset);
					}
				});
			}
			return null;
		}
	    
	    /**
	     * 减去指定的值
	     * Increment value of key by value. 
	     * @param key
	     * @param integer
	     * @return
	     */
	    public Long decrBy(final String key,final long integer) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.decrBy(keys, integer);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 对key的值做减减操作,如果key不存在,则设置key为-1
	     * Decrement value of key by 1. 
	     * @param key
	     * @return
	     */
	    public Long decr(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.decr(keys);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key给指定的value加值,如果key不存在,则这时value为该值
	     * Increment value of key by value. 
	     * @param key
	     * @param integer
	     * @return
	     */
	    public Long incrBy(final String key,final long integer) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.incrBy(keys,integer);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key 对value进行加值+1操作,当value不是int类型时会返回错误,当key不存在是则value为1
	     * Increment value of key by 1. 
	     * @param key
	     * @return 加值后的结果 
	     */
	    public Long incr(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.incr(keys);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key向指定的value值追加内容
	     * Append a value to key. 
	     * @param key
	     * @param value
	     * @return 成功返回 添加后value的长度 失败 返回 添加的 value 的长度  异常返回0L
	     */
	    public Long append(final String key,final String value) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] values = serializer.serialize(value);
						return connection.append(keys,values);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key给field设置指定的值,如果key不存在,则先创建。（hash操作）
	     * Set the value of a hash field.
	     * @param key
	     * @param field
	     * @param value
	     * @return 如果存在返回0 异常返回null 
	     */
	    public Boolean hSet(final String key,final String field,final String value) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] fields = serializer.serialize(field);
						byte[] values = serializer.serialize(value);
						return connection.hSet(keys, fields, values);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key 和 field 获取指定的 value
	     * Get value for given field from hash at key.
	     * @param key
	     * @param field
	     * @return 没有返回null 
	     */
	    public String hGet(final String key,final String field) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<byte[]>() {
					public byte[] doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] fields = serializer.serialize(field);
						return connection.hGet(keys, fields);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key给field设置指定的值,如果key不存在则先创建,如果field已经存在,返回0
	     * Set the value of a hash field only if field does not exist.
	     * @param key
	     * @param field
	     * @param value
	     * @return
	     */
	    public Long hSetNX(final String key,final String field,final String value) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] fields = serializer.serialize(field);
						byte[] values = serializer.serialize(value);
						return connection.hSetNX(keys, fields, values);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key同时设置 hash的多个field
	     * Set multiple hash fields to multiple values using data provided in hashes
	     * @param key
	     * @param hash
	     * @return 返回TRUE 异常返回null 
	     */
	    public Boolean hMSet(final String key,final Map<byte[], byte[]> hash) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						connection.hMSet(keys, hash);
						return true;
					}
				});
			}
	    	return false;
	    }
	    
	    /**
	     * 返回哈希表 key 中，一个或多个给定域的值
	     * 通过key 和 fields 获取指定的value 如果没有对应的value则返回null
	     * Get values for given fields from hash at key.
	     * @param key
	     * @param fields 可以是一个String 也可以是 String数组
	     * @return
	     */
	    public List<String> hMGet(final String key,final byte[]... fields) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<List<String>>() {
					public List<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						List<String> data = new ArrayList<>();
						byte[] keys = serializer.serialize(key);
						List<byte[]> re = connection.hMGet(keys, fields);
						for(byte[] by : re){
							data.add(serializer.deserialize(by));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key给指定的field的value加上给定的值
	     * Increment value of a hash field by the given delta.
	     * @param key
	     * @param field
	     * @param value
	     * @return
	     */
	    public Long hIncrBy(final String key,final String field,final long value) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] fields = serializer.serialize(field);
						return connection.hIncrBy(keys, fields, value);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key和field判断是否有指定的value存在
	     * Determine if given hash field exists.
	     * @param key
	     * @param field
	     * @return
	     */
	    public Boolean hexists(final String key,final String field) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] fields = serializer.serialize(field);
						return connection.hExists(keys, fields);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key 删除指定的 field
	     * Delete given hash fields.
	     * @param key
	     * @param field fields 可以是 一个 field 也可以是 一个数组 
	     * @return
	     */
	    public Long hDel(final String key,final byte[]... fields) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
//						byte[] fields = serializer.serialize(field);
						return connection.hDel(keys, fields);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key返回field的数量
	     * Get size of hash at key.
	     * @param key
	     * @return
	     */
	    public Long hLen(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.hLen(keys);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key返回所有的field
	     * Get key set (fields) of hash at key.
	     * @param key
	     * @return
	     */
	    public Set<String> hKeys(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<String>>() {
					public Set<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						Set<String> data = new HashSet<>();
						Set<byte[]> re = connection.hKeys(keys);
						for(byte[] by : re){
							data.add(serializer.deserialize(by));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key返回所有和key有关的value
	     * Get entry set (values) of hash at field.
	     * @param key
	     * @return
	     */
	    public List<String> hVals(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<List<String>>() {
					public List<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						List<String> data = new ArrayList<>();
						List<byte[]> re = connection.hVals(keys);
						for(byte[] by : re){
							data.add(serializer.deserialize(by));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key获取所有的field和value
	     * Get entire hash stored at key.
	     * @param key
	     * @return
	     */
	    public Map<byte[], byte[]> hGetAll(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Map<byte[], byte[]>>() {
					public Map<byte[], byte[]> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						 return connection.hGetAll(keys);
					}
				});
			}
	    	return null;
	    }
	    
	 // ================list ====== l表示 list或 left, r表示right====================
	    /**
	     * 通过key向list尾部添加字符串
	     * Append values to key. 
	     * @param key
	     * @param value 可以是一个string 也可以是string数组 
	     * @return 返回list的value个数
	     */
	    public Long rPush(final String key,final String value) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] values = serializer.serialize(value);
						return connection.rPush(keys,values);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key返回list的长度
	     * Get the size of list stored at key. 
	     * @param key
	     * @return
	     */
	    public Long lLen(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.lLen(keys);
					}
				});
			}
	    	return null;
	    }
	    
		/**
		 * 根据参数 count 的值，移除列表中与参数 value 相等的元素
		 * @param keyStr
		 * @param count
		 * @param valueStr
		 * @return
		 */
		public Long lrem(final String key, final long count, final String value) {
			if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] values = serializer.serialize(value);
						return connection.lRem(keys, count, values);
					}
				});
			}
			return null;
	    }
		
		/**
		 * 将一个或多个值 value 插入到列表 key 的表头
		 * @param keyStr
		 * @param valueStr
		 * @return
		 */
		public Long lpush(final String key, final String value) {
			if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] values = serializer.serialize(value);
						return connection.lPush(keys, values);
					}
				});
			}
			return null;
	    }
	    
	    /**
	     * 通过key获取list指定下标位置的value
	     * 如果start 为 0 end 为 -1 则返回全部的list中的value
	     * Get elements between begin and end from list at key. 
	     * @param key
	     * @param start
	     * @param end
	     * @return
	     */
	    public List<String> lRange(final String key,final Long start,final Long end) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<List<String>>() {
					public List<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						List<String> data = new ArrayList<>();
						List<byte[]> re = connection.lRange(keys, start, end);
						for(byte[] by : re){
							data.add(serializer.deserialize(by));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key保留list中从strat下标开始到end下标结束的value值
	     * Trim list at key to elements between begin and end. 
	     * @param key
	     * @param start
	     * @param end
	     * @return 成功返回TRUE
	     */
	    public Boolean ltrim(final String key,final long start,final long end) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						connection.lTrim(keys, start, end);
						return true;
					}
				});
			}
	    	return false;
	    }
	    
	    /**
	     * 通过key获取list中指定下标位置的value
	     * Get element at index form list at key. 
	     * @param key
	     * @param index
	     * @return 如果没有返回null 
	     */
	    public String lIndex(final String key,final long index) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<String>() {
					public String doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] re = connection.lIndex(keys, index);
						return serializer.deserialize(re);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key设置list指定下标位置的value
	     * 如果下标超过list里面value的个数则报错
	     * Set the value list element at index. 
	     * @param key
	     * @param index 从0开始
	     * @param value
	     * @return 成功返回TRUE 
	     */
	    public Boolean lSet(final String key,final long index,final String value) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] values = serializer.serialize(value);
						connection.lSet(keys, index, values);
						return true;
					}
				});
			}
	    	return false;
	    }
	    
	    /**
	     * 通过key在list指定的位置之前或者之后 添加字符串元素
	     * Insert value RedisListCommands.Position.BEFORE or RedisListCommands.Position.AFTER existing pivot for key. 
	     * @param key
	     * @param where LIST_POSITION枚举类型 
	     * @param pivot list里面的value 
	     * @param value 添加的value
	     * @return
	     */
	    public Long linsert(final String key, final Position where,final String pivot,final String value) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] pivots = serializer.serialize(pivot);
						byte[] values = serializer.serialize(value);
						return connection.lInsert(keys, where, pivots, values);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key从list的头部删除一个value,并返回该value
	     * Removes and returns first element in list stored at key. 
	     * @param key
	     * @return
	     */
	    public String lPop(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<String>() {
					public String doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] re = connection.lPop(keys);
						return serializer.deserialize(re);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key从list尾部删除一个value,并返回该元素
	     * Removes and returns last element in list stored at key. 
	     * @param key
	     * @return
	     */
	    public String rPop(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<String>() {
					public String doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] re = connection.rPop(keys);
						return serializer.deserialize(re);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key向指定的set中添加value
	     * 
	     * @param key
	     * @param member 可以是一个String 也可以是一个String数组
	     * @return
	     */
	    public Long sAdd(final String key,final String member) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] members = serializer.serialize(member);
						return connection.sAdd(keys,members);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key获取set中所有的value
	     * Get all elements of set at key. 
	     * @param key
	     * @return
	     */
	    public Set<String> sMembers(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<String>>() {
					public Set<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						Set<byte[]> set = connection.sMembers(keys);
						Set<String> data = new HashSet<>();
						for (byte[] s :set) {
							data.add(serializer.deserialize(s));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key删除set中对应的value值
	     * Remove given values from set at key and return the number of removed elements. 
	     * @param key
	     * @param member 可以是一个String 也可以是一个String数组 
	     * @return 删除的个数
	     */
	    public Long sRem(final String key,final String member) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] members = serializer.serialize(member);
						return connection.sRem(keys,members);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key随机删除一个set中的value并返回该值
	     * Remove and return a random member from set at key. 
	     * @param key
	     * @return
	     */
	    public String sPop(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<String>() {
					public String doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] re = connection.sPop(keys);
						return serializer.deserialize(re);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key获取set中value的个数
	     * Get size of set at key. 
	     * @param key
	     * @return
	     */
	    public Long sCard(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.sCard(keys);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key判断value是否是set中的元素
	     * Check if set at key contains value. 
	     * @param key
	     * @param member
	     * @return
	     */
	    public Boolean sIsMember(final String key,final String member) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] members = serializer.serialize(member);
						return connection.sIsMember(keys,members);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key获取set中随机的value,不删除元素
	     * Get random element from set at key. 
	     * @param key
	     * @return
	     */
	    public String sRandMember(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<String>() {
					public String doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] re = connection.sRandMember(keys);
						return serializer.deserialize(re);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key向zset中添加value,score,其中score就是用来排序的
	     * 如果该value已经存在则根据score更新元素
	     * Add value to a sorted set at key, or update its score if it already exists. 
	     * @param key
	     * @param score
	     * @param member
	     * @return
	     */
	    public Boolean zAdd(final String key,final double score,final String member) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Boolean>() {
					public Boolean doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] members = serializer.serialize(member);
						return connection.zAdd(keys, score, members);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 根据begin end范围获取排序set的元素
	     * Get elements between begin and end from sorted set. 
	     * @param key
	     * @param start
	     * @param end
	     * @return
	     */
	    public Set<String> zRange(final String key,final int start,final int end) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<String>>() {
					public Set<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						Set<byte[]> set = connection.zRange(keys, start, end);
						Set<String> data = new HashSet<>();
						for (byte[] s : set) {
							data.add(serializer.deserialize(s));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key删除在zset中指定的value
	     * Remove values from sorted set. Return number of removed elements. 
	     * @param key
	     * @param member 可以是一个string 也可以是一个string数组 
	     * @return
	     */
	    public Long zRem(final String key,final String member) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] members = serializer.serialize(member);
						return connection.zRem(keys , members);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key增加该zset中value的score的值
	     * Increment the score of element with value in sorted set by increment. 
	     * @param key
	     * @param score
	     * @param member
	     * @return
	     */
	    public Double zIncrBy(final String key,final double score,final String member) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Double>() {
					public Double doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] members = serializer.serialize(member);
						return connection.zIncrBy(keys, score, members);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key返回zset中value的排名
	     * 下标从小到大排序
	     * Determine the index of element with value in a sorted set. 
	     * @param key
	     * @param member
	     * @return
	     */
	    public Long zRank(final String key,final String member) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] members = serializer.serialize(member);
						return connection.zRank(keys , members);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key返回zset中value的排名
	     * 下标从大到小排序
	     * Determine the index of element with value in a sorted set when scored high to low. 
	     * @param key
	     * @param member
	     * @return
	     */
	    public Long zRevRank(final String key,final String member) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] members = serializer.serialize(member);
						return connection.zRevRank(keys , members);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key将获取score从start到end中zset的value
	     * socre从大到小排序
	     * 当start为0 end为-1时返回全部
	     * Get elements in range from begin to end from sorted set ordered from high to low. 
	     * @param key
	     * @param start
	     * @param end
	     * @return
	     */
	    public Set<String> zRevRange(final String key,final int start,final int end) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<String>>() {
					public Set<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						Set<byte[]> set = connection.zRevRange(keys, start, end);
						Set<String> data = new HashSet<>();
						for (byte[] s : set) {
							data.add(serializer.deserialize(s));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get set of RedisZSetCommands.Tuples between begin and end from sorted set. 
	     * @param key
	     * @param start
	     * @param end
	     * @return
	     */
	    public Set<Tuple> zRangeWithScores(final String key,final int start,final int end) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
					public Set<Tuple> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zRangeWithScores(keys, start, end);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get set of RedisZSetCommands.Tuples in range from begin to end from sorted set ordered from high to low. 
	     * @param key
	     * @param start
	     * @param end
	     * @return
	     */
	    public Set<Tuple> zRevRangeWithScores(final String key,final int start,final int end) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
					public Set<Tuple> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zRevRangeWithScores(keys, start, end);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key返回zset中的value个数
	     * Get the size of sorted set with key. 
	     * @param key
	     * @return
	     */
	    public Long zCard(final String key) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zCard(keys);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key获取zset中value的score值
	     * Get the score of element with value from sorted set with key key. 
	     * @param key
	     * @param member
	     * @return
	     */
	    public Double zScore(final String key,final String member) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Double>() {
					public Double doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						byte[] members = serializer.serialize(member);
						return connection.zScore(keys , members);
					}
				});
			}
	    	return null;
	    }
	    
	   
	    
	    /**
	     * 返回指定区间内zset中value的数量
	     * Count number of elements within sorted set with scores between Range#min and Range#max.
	     * @param key
	     * @param min
	     * @param max
	     * @return
	     */
	    public Long zCount(final String key,final double min,final double max) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zCount(keys, min, max);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key返回指定score内zset中的value
	     * Get elements where score is between min and max from sorted set ordered from high to low.
	     * @param key
	     * @param max
	     * @param min
	     * @return
	     */
	    public Set<String> zrevrangeByScore(final String key,final double max,final double min) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<String>>() {
					public Set<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						Set<byte[]> set = connection.zRevRangeByScore(keys, max, min);
						Set<String> data = new HashSet<>();
						for (byte[] s : set) {
							data.add(serializer.deserialize(s));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get elements in range from begin to end where score is between min and max from sorted set ordered high -> low.
	     * @param key
	     * @param max
	     * @param min
	     * @param offset
	     * @param count
	     * @return
	     */
	    public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset, final int count) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<String>>() {
					public Set<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						Set<byte[]> set = connection.zRevRangeByScore(keys, min, max, offset, count);
						Set<String> data = new HashSet<>();
						for (byte[] s : set) {
							data.add(serializer.deserialize(s));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get set of RedisZSetCommands.Tuples where score is between min and max from sorted set.
	     * @param key
	     * @param min
	     * @param max
	     * @return
	     */
	    public Set<Tuple> zrangeByScoreWithScores(final String key,final double min,final double max) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
					public Set<Tuple> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						 return connection.zRangeByScoreWithScores(keys, min, max);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get set of RedisZSetCommands.Tuple where score is between min and max from sorted set ordered from high to low.
	     * @param key
	     * @param max
	     * @param min
	     * @return
	     */
	    public Set<Tuple> zrevrangeByScoreWithScores(final String key,final double max,final double min) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
					public Set<Tuple> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						 return connection.zRevRangeByScoreWithScores(keys, min, max);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get set of RedisZSetCommands.Tuples in range from begin to end where score is between min and max from sorted set.
	     * @param key
	     * @param min
	     * @param max
	     * @param offset
	     * @param count
	     * @return
	     */
	    public Set<Tuple> zrangeByScoreWithScores(final String key,final double min,final double max,final int offset,final int count) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
					public Set<Tuple> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zRangeByScoreWithScores(keys, min, max,offset,count);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get set of RedisZSetCommands.Tuple in range from begin to end where score is between min and max from sorted set ordered high -> low.
	     * @param key
	     * @param max
	     * @param min
	     * @param offset
	     * @param count
	     * @return
	     */
	    public Set<Tuple> zrevrangeByScoreWithScores(final String key,final double max,final double min,final int offset,final int count) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
					public Set<Tuple> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zRevRangeByScoreWithScores(keys, max, min, offset, count);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key删除指定score内的元素
	     * Remove elements with scores between min and max from sorted set with key.
	     * @param key
	     * @param start
	     * @param end
	     * @return
	     */
	    public Long zremrangeByScore(final String key,final double start,final double end) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zRemRangeByScore(keys, start, end);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * 通过key返回指定score内zset中的value
	     * Get elements where score is between min and max from sorted set.
	     * @param key
	     * @param min
	     * @param max
	     * @return
	     */
	    public Set<String> zRangeByScore(final String key,final double min,final double max) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<String>>() {
					public Set<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						Set<byte[]> set = connection.zRangeByScore(keys, max, min);
						Set<String> data = new HashSet<>();
						for (byte[] s : set) {
							data.add(serializer.deserialize(s));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get elements in range from begin to end where score is between min and max from sorted set.
	     * @param key
	     * @param min
	     * @param max
	     * @param offset
	     * @param count
	     * @return
	     */
	    public Set<String> zRangeByScore(final String key,final double min,final double max,final int offset,final int count) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<String>>() {
					public Set<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						Set<byte[]> set = connection.zRangeByScore(keys, max, min,offset,count);
						Set<String> data = new HashSet<>();
						for (byte[] s : set) {
							data.add(serializer.deserialize(s));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get set of RedisZSetCommands.Tuples where score is between min and max from sorted set.
	     * @param key
	     * @param min
	     * @param max
	     * @return
	     */
	    public Set<Tuple> zRangeByScoreWithScores(final String key,final double min,final double max) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
					public Set<Tuple> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zRangeByScoreWithScores(keys, max, min);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get set of RedisZSetCommands.Tuples in range from begin to end where score is between min and max from sorted set.
	     * @param key
	     * @param min
	     * @param max
	     * @param offset
	     * @param count
	     * @return
	     */
	    public Set<Tuple> zRangeByScoreWithScores(final String key,final double min,final double max,final int offset,final int count) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
					public Set<Tuple> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zRangeByScoreWithScores(keys, max, min,offset,count);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get elements where score is between min and max from sorted set ordered from high to low.
	     * @param key
	     * @param max
	     * @param min
	     * @return
	     */
	    public Set<String> zRevRangeByScore(final String key,final double max,final double min) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<String>>() {
					public Set<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						Set<byte[]> set = connection.zRevRangeByScore(keys, max, min);
						Set<String> data = new HashSet<>();
						for (byte[] s : set) {
							data.add(serializer.deserialize(s));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get elements in range from begin to end where score is between min and max from sorted set ordered high -> low.
	     * @param key
	     * @param max
	     * @param min
	     * @param offset
	     * @param count
	     * @return
	     */
	    public Set<String> zRevRangeByScore(final String key,final double max,final double min,final int offset,final int count) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<String>>() {
					public Set<String> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						Set<byte[]> set = connection.zRevRangeByScore(keys, max, min,offset,count);
						Set<String> data = new HashSet<>();
						for (byte[] s : set) {
							data.add(serializer.deserialize(s));
						}
						return data;
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get set of RedisZSetCommands.Tuple where score is between min and max from sorted set ordered from high to low.
	     * @param key
	     * @param max
	     * @param min
	     * @return
	     */
	    public Set<Tuple> zRevRangeByScoreWithScores(final String key,final double max,final double min) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
					public Set<Tuple> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zRevRangeByScoreWithScores(keys, max, min);
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Get set of RedisZSetCommands.Tuple in range from begin to end where score is between min and max from sorted set ordered high -> low.
	     * @param key
	     * @param max
	     * @param min
	     * @param offset
	     * @param count
	     * @return
	     */
	    public Set<Tuple> zRevRangeByScoreWithScores(final String key,final  double max,final double min,final int offset,final int count) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
					public Set<Tuple> doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zRevRangeByScoreWithScores(keys,max,min, offset,count );
					}
				});
			}
	    	return null;
	    }
	    
	    /**
	     * Remove elements with scores between min and max from sorted set with key.
	     * @param key
	     * @param start
	     * @param end
	     * @return
	     */
	    public Long zRemRangeByScore(final String key,final double start,final double end) {
	    	if (redisTemplate != null) {
				redisTemplate.execute(new RedisCallback<Long>() {
					public Long doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();
						byte[] keys = serializer.serialize(key);
						return connection.zRemRangeByScore(keys, start, end);
					}
				});
			}
	    	return null;
	    }
	    
}



