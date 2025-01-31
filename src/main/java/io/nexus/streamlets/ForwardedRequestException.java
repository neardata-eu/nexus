package io.nexus.streamlets;

public class ForwardedRequestException extends RuntimeException {
    public ForwardedRequestException(String message) {
        super(message);
    }
}
