package com.flab.stocktradingengine.user.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "user.token")
public class UserTokenProperties {

    /** Redis 토큰 TTL(초). null 또는 0 이하면 기본 7일 사용 */
    private Long expireSeconds;
}
