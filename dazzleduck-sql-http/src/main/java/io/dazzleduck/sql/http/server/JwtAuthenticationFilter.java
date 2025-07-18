package io.dazzleduck.sql.http.server;

import com.typesafe.config.Config;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.http.UnauthorizedException;
import io.helidon.webserver.http.Filter;
import io.helidon.webserver.http.FilterChain;
import io.helidon.webserver.http.RoutingRequest;
import io.helidon.webserver.http.RoutingResponse;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;

import javax.crypto.SecretKey;
import java.util.Date;

public class JwtAuthenticationFilter implements Filter {
    public static final String USER_CONTEXT_KEY = "user";

    private static final int BEARER_LENGTH = "Bearer ".length();
    private final Config config;
    private final SecretKey secretKey;
    private final JwtParser jwtParser;
    private final String path;
    public JwtAuthenticationFilter(String path, Config config, SecretKey secretKey) {
        this.config = config;
        this.secretKey = secretKey;
        this.jwtParser = Jwts.parser()     // (1)
                .verifyWith(secretKey)      //     or a constant key used to verify all signed JWTs
                .build();
        this.path = path;
    }

    public String authenticate(String token) {
        try {
            var jwt = jwtParser.parseSignedClaims(token);
            var payload = jwt.getPayload();
            var subject = payload.getSubject();
            var expiration = payload.getExpiration();
            if (expiration.after(new Date())) {
                return subject;
            }
            throw new UnauthorizedException("jwt expired for subject :" + subject);
        } catch (Exception e) {
            throw new UnauthorizedException("invalid jwt");
        }
    }

    @Override
    public void filter(FilterChain chain, RoutingRequest req, RoutingResponse res) {
        if( !req.path().path().startsWith(path)) {
            chain.proceed();
            return;
        }
        var token = req.headers().value(HeaderNames.AUTHORIZATION);
        if (token.isEmpty()) {
            res.status(Status.UNAUTHORIZED_401);
            res.send();
        } else {
            try {
                var user = authenticate(removeBearer(token.get()));
                req.context().register(USER_CONTEXT_KEY, user);
                chain.proceed();
            } catch (UnauthorizedException unauthorizedException) {
                res.status(Status.UNAUTHORIZED_401);
                res.send(unauthorizedException.getMessage().getBytes());
            }
        }
    }

    private static String removeBearer(String input) {
        return input.substring(BEARER_LENGTH);
    }
}
