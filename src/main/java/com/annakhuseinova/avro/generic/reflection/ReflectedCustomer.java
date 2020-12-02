package com.annakhuseinova.avro.generic.reflection;

import org.apache.avro.reflect.Nullable;

public class ReflectedCustomer {

    private String firstName;
    private String lastName;
    @Nullable
    private String nickName;

    public ReflectedCustomer(){

    }

    public ReflectedCustomer(String firstName, String lastName, String nickName) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.nickName = nickName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getNickName() {
        return nickName;
    }
}
