package com.hcsc.bridge.parser;

import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.model.ParsedPayload;

public interface MessageParser {

    ParsedPayload parse(MqMessage message) throws MessageParseException;
}
