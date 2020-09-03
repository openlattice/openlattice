package com.openlattice.codex.controllers

import org.apache.olingo.commons.api.edm.FullQualifiedName

class CodexConstants() {

    companion object {
        val COLLECTION_FQN = FullQualifiedName("app.codex")
        const val APP_NAME = "codex"
    }

    enum class CollectionTemplateType(val typeName: String) {
        PEOPLE("app.people"),
        MESSAGES("app.messages"),
        CONTACT_INFO("app.contactinformation"),
        SENT_FROM("app.sentfrom"),
        SENT_TO("app.sentto")
    }

    enum class PropertyType(val fqn: FullQualifiedName) {
        ID(FullQualifiedName("ol.id")),
        PHONE_NUMBER(FullQualifiedName("contact.phonenumber")),
        IMAGE_DATA(FullQualifiedName("ol.imagedata")),
        TEXT(FullQualifiedName("ol.text")),
        TYPE(FullQualifiedName("ol.type")),
        DATE_TIME(FullQualifiedName("general.datetime")),
        WAS_DELIVERED(FullQualifiedName("ol.delivered")),
        CHANNEL(FullQualifiedName("ol.channel")),
        IS_OUTGOING(FullQualifiedName("ol.isoutgoing")),

        PERSON_ID(FullQualifiedName("nc.SubjectIdentification")),
        NICKNAME(FullQualifiedName("im.PersonNickName"))
    }

    enum class Request(val parameter: String) {
        SID("MessageSid"),
        FROM("From"),
        TO("To"),
        BODY("Body"),
        STATUS("MessageStatus"),
        NUM_MEDIA("NumMedia"),
        MEDIA_TYPE_PREFIX("MediaContentType"),
        MEDIA_URL_PREFIX("MediaUrl")
    }
}