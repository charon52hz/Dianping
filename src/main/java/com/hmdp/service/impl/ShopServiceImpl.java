package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;


@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
//        Result shop = queryWithPassThrough(id); //缓存穿透

        Result shop = queryWithMutex(id);   //缓存击穿
        return Result.ok(shop);
    }

    public Result queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (shopJson != null && !"".equals(shopJson)) { //不等于null，且不等于""
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }
        if (shopJson!=null){    //缓存的是""
            return Result.fail("店铺不存在");
        }

        Shop shop = getById(id);
        if (shop == null){
            stringRedisTemplate.opsForValue().set(key,"",2L,TimeUnit.MINUTES);
            return Result.fail("店铺不存在");
        }
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop));
        return Result.ok(shop);
    }


    public Result queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        Shop shop = null;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (shopJson != null && !"".equals(shopJson)) { //不等于null，且不等于""
            shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }
        if (shopJson!=null){    //缓存的是""
            return Result.fail("店铺不存在");
        }

        //未命中redis缓存
        String lockKey = "lock:shop:"+id;

        try {
            boolean lock = tryLock(lockKey);
            if (!lock){
                Thread.sleep(10);
                return queryWithMutex(id);
            }

            shopJson = stringRedisTemplate.opsForValue().get(key);  //双重校验
            if (shopJson == null){
                shop = getById(id);
                Thread.sleep(200);  //模拟查询数据库重建业务
                if (shop == null){
                    stringRedisTemplate.opsForValue().set(key,"",2L,TimeUnit.MINUTES);
                    return Result.fail("店铺不存在");
                }
                stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(lockKey);
        }

        return Result.ok(shop);
    }


    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);    //拆箱,防止空指针异常
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
