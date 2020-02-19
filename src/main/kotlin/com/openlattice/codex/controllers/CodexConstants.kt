package com.openlattice.codex.controllers

import org.apache.olingo.commons.api.edm.FullQualifiedName

class CodexConstants() {

    companion object {
        val COLLECTION_FQN = FullQualifiedName("app.codex")
    }

    enum class CollectionTemplateType(val typeName: String) {
        PEOPLE("app.people"),
        MESSAGES("app.messages"),
        CONTACT_INFO("app.contactinformation"),
        SETTINGS("app.settings"),
        SENT_FROM("app.sentfrom"),
        SENT_TO("app.sentto"),
        SUBJECT_OF("app.subjectof"),
        CONTACT_INFO_GIVEN("app.contactinfogiven")
    }

    enum class PropertyType(val fqn: FullQualifiedName) {
        ID(FullQualifiedName("ol.id")),
        PHONE_NUMBER(FullQualifiedName("contact.phonenumber")),
        TEXT(FullQualifiedName("ol.text")),
        TYPE(FullQualifiedName("ol.type")),
        DATE_TIME(FullQualifiedName("general.datetime")),
        WAS_DELIVERED(FullQualifiedName("ol.delivered")),
        CHANNEL(FullQualifiedName("ol.channel")),

        PERSON_ID(FullQualifiedName("nc.SubjectIdentification")),
        NICKNAME(FullQualifiedName("im.PersonNickName"))
    }

    enum class Request(val parameter: String) {
        SID("MessageSid"),
        FROM("From"),
        TO("To"),
        BODY("Body"),
        STATUS("MessageStatus")
    }
}