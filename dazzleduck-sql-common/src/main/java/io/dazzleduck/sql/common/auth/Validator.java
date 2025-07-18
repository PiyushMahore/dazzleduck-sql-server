package io.dazzleduck.sql.common.auth;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.helidon.http.UnauthorizedException;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface Validator {
    public static byte[] hash(String originalString) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(
                    originalString.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean passwordMatch(byte[] aArray, byte[] bArray) {
        var len = Math.min(aArray.length, bArray.length);
        var diff = 0;
        for(int i = 0 ; i < len; i ++){
            if(aArray[i] != bArray[i]){
                diff ++;
            }
        }
        diff = diff + Math.abs(aArray.length - bArray.length);
        return diff == 0;
    }

    public static SecretKey generateRandoSecretKey() throws NoSuchAlgorithmException {
        var secureKeySize = 32;
        byte[] secureRandomBytes = new byte[secureKeySize];
        SecureRandom.getInstanceStrong().nextBytes(secureRandomBytes);
        return Keys.hmacShaKeyFor(secureRandomBytes);
    }

    public static boolean validatePassword(String username, String password, Map<String, byte[]> userHashMap) {
        var storePassword = userHashMap.get(username);
        return storePassword != null &&
                !password.isEmpty() &&
                Validator.passwordMatch(storePassword, Validator.hash(password));
    }

    boolean validate(String username, String password) throws Exception;

    public static Validator load(Config config) {
        List<? extends ConfigObject> users = config.getObjectList("users");
        final Map<String, byte[]> passwords = new HashMap<>();
        users.forEach( o -> {
            String name = o.toConfig().getString("username");
            String password = o.toConfig().getString("password");
            passwords.put(name, hash(password));
        });
        return new Validator() {
            @Override
            public boolean validate(String username, String password) throws Exception {
                if( !validatePassword(username, password, passwords)) {
                    throw new UnauthorizedException(username);
                };
                return true;
            }
        };
    }
}
