package com.enterprise.bridge.parser;

import com.enterprise.bridge.model.MqMessage;
import com.enterprise.bridge.model.ParsedPayload;

public interface MessageParser {

    ParsedPayload parse(MqMessage message) throws MessageParseException;
}
