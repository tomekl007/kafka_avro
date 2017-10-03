package com.tomekl007.kafka.serializer;

import com.tomekl007.avro.User;
import org.junit.Ignore;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

import static org.assertj.core.api.Assertions.assertThat;


public class AvroDeserializerTest {

    @Test
    public void testDeserialize() {
        //given
        User user = User.newBuilder().setName("John Doe").setFavoriteColor("green")
                .setFavoriteNumber(null).build();

        //when
        byte[] data = DatatypeConverter.parseHexBinary("104A6F686E20446F6502000A677265656E");
        AvroDeserializer<User> avroDeserializer = new AvroDeserializer<>(User.class);

        //then
        assertThat(avroDeserializer.deserialize("avro.t", data)).isEqualTo(user);
        avroDeserializer.close();
    }
}
