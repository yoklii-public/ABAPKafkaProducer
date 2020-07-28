*&---------------------------------------------------------------------*
*& Report ZTCP_KAFKA_PRODUCER
*&---------------------------------------------------------------------*
*& https://kafka.apache.org/protocol
*& https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
*& https://help.sap.com/http.svc/rc/PRODUCTION/568a51b6ea68442d81880c393bf4b7b1/1610%20000/en-US/frameset.htm?b8c7a04b032542a6ad9ac01d3c9e50a0.html
*& https://help.sap.com/http.svc/rc/PRODUCTION/568a51b6ea68442d81880c393bf4b7b1/1610%20000/en-US/frameset.htm?4f3f842b2e2447789c3a2ad1e5b67668.html
*&---------------------------------------------------------------------*
REPORT ZTCP_KAFKA_PRODUCER.
PARAMETERS: host    TYPE string LOWER CASE DEFAULT 'mylead.kafkabroker.domain.dom',
            port    TYPE string DEFAULT '9092',
            topic   TYPE string LOWER CASE DEFAULT 'Test.ABAP.TestTopic1',
            clientid TYPE string LOWER CASE DEFAULT 'abap-producer',
            message TYPE string LOWER CASE DEFAULT 'Say hello!'.

CLASS lcl_serialize DEFINITION.
  PUBLIC SECTION.
    CLASS-METHODS:
      serialize_int1
        IMPORTING iv_int1 type int1
        RETURNING VALUE(result_xstring) type xstring.
    CLASS-METHODS:
      serialize_int2
        IMPORTING iv_int2 type int2
        RETURNING VALUE(result_xstring) type xstring.
    CLASS-METHODS:
      serialize_int
        IMPORTING iv_int type i
        RETURNING VALUE(result_xstring) type xstring.
    CLASS-METHODS:
      serialize_int8
        IMPORTING iv_int8 type int8
        RETURNING VALUE(result_xstring) type xstring.
    CLASS-METHODS:
      serialize_string
        IMPORTING iv_string type string
        RETURNING VALUE(result_xstring) type xstring.
    CLASS-METHODS:
      serialize_string_as_bytes
        IMPORTING iv_string type string
        RETURNING VALUE(result_xstring) type xstring.
    CLASS-METHODS:
      pop_string
        EXPORTING ev_string type string
        CHANGING iv_bytes type xstring.
    CLASS-METHODS:
      pop_int
        EXPORTING ev_int type i
        CHANGING iv_bytes type xstring.
    CLASS-METHODS:
      pop_int2
        EXPORTING ev_int2 type int2
        CHANGING iv_bytes type xstring.
    CLASS-METHODS:
      pop_int8
        EXPORTING ev_int8 type int8
        CHANGING iv_bytes type xstring.

ENDCLASS.

CLASS lcl_serialize IMPLEMENTATION.
    METHOD serialize_int1.
      DATA: lv_x  TYPE x length 1.  "1 byte integer for message size
      lv_x = iv_int1.
      result_xstring = lv_x.
    ENDMETHOD.


    METHOD serialize_int2.
      DATA: lv_x  TYPE x length 2.  "2 byte integer for message size
      lv_x = iv_int2.
      result_xstring = lv_x.
    ENDMETHOD.

    METHOD serialize_int.
      DATA: lv_x  TYPE x length 4.  "4 byte integer for message size
      lv_x = iv_int.
      result_xstring = lv_x.
    ENDMETHOD.

    METHOD serialize_int8.
      DATA: lv_x  TYPE x length 8.  "8 byte integer for message size
      lv_x = iv_int8.
      result_xstring = lv_x.
    ENDMETHOD.

    METHOD serialize_string.
      "put a 2 byte length value in front of string bytes
      DATA(lv_xstring) = cl_abap_codepage=>convert_to( source = iv_string ).
      DATA lv_len TYPE int2.
      lv_len = XSTRLEN( lv_xstring ).
      IF lv_len = 0.
        lv_len = -1.
      ENDIF.
      DATA(lv_len_xstring) = lcl_serialize=>serialize_int2( lv_len ).
      CONCATENATE lv_len_xstring lv_xstring INTO result_xstring IN BYTE MODE.
    ENDMETHOD.


    METHOD serialize_string_as_bytes.
      "put a 4 byte length value in front of bytes
      DATA(lv_xstring) = cl_abap_codepage=>convert_to( source = iv_string ).
      DATA(lv_len) = XSTRLEN( lv_xstring ).
      IF lv_len = 0.
        lv_len = -1.
      ENDIF.
      DATA(lv_len_xstring) = lcl_serialize=>serialize_int( lv_len ).
      CONCATENATE lv_len_xstring lv_xstring INTO result_xstring IN BYTE MODE.
    ENDMETHOD.

    METHOD pop_string.
      "read the 2 byte length
      DATA: lv_xstring TYPE xstring,
           lv_len TYPE int2.
      lv_xstring = iv_bytes+0(2).
      MOVE lv_xstring TO lv_len.
      lv_xstring = iv_bytes+2(lv_len).
      MOVE lv_xstring to ev_string.
      DATA(lv_in_length) = xstrlen( iv_bytes ).
      lv_len = lv_len + 2.
      lv_in_length = lv_in_length - lv_len.
      iv_bytes = iv_bytes+lv_len(lv_in_length).
    ENDMETHOD.


    METHOD pop_int.
      "read the 4 byte int
      DATA lv_xstring TYPE xstring.
      lv_xstring = iv_bytes+0(4).
      MOVE lv_xstring TO ev_int.
      DATA(lv_in_length) = xstrlen( iv_bytes ).
      lv_in_length = lv_in_length - 4.
      iv_bytes = iv_bytes+4(lv_in_length).
    ENDMETHOD.


    METHOD pop_int2.
      "read the 2 byte int
      DATA lv_xstring TYPE xstring.
      lv_xstring = iv_bytes+0(2).
      MOVE lv_xstring TO ev_int2.
      DATA(lv_in_length) = xstrlen( iv_bytes ).
      lv_in_length = lv_in_length - 2.
      iv_bytes = iv_bytes+2(lv_in_length).
    ENDMETHOD.

    METHOD pop_int8.
      "read the 8 byte int
      DATA lv_xstring TYPE xstring.
      lv_xstring = iv_bytes+0(8).
      MOVE lv_xstring TO ev_int8.
      DATA(lv_in_length) = xstrlen( iv_bytes ).
      lv_in_length = lv_in_length - 8.
      iv_bytes = iv_bytes+8(lv_in_length).
    ENDMETHOD.


ENDCLASS.

*&---------------------------------------------------------------------*
*&       Interface lif_kafka_serializable_object
*&---------------------------------------------------------------------*
*
*----------------------------------------------------------------------*
INTERFACE lif_kafka_serializable_object.
   METHODS serialize RETURNING VALUE(result_xstring) type xstring.
ENDINTERFACE.

TYPES tab_kafka_serializable TYPE TABLE OF REF TO lif_kafka_serializable_object.
TYPES:
  begin of lty_tab_kafka_serial,
    instance TYPE REF TO lif_kafka_serializable_object,
  end of lty_tab_kafka_serial.


CLASS lcl_kafka_request_header DEFINITION.
  PUBLIC SECTION.
    INTERFACES: lif_kafka_serializable_object.
    METHODS:
      constructor
        IMPORTING api_key TYPE int2
                  api_version TYPE int2
                  correlation_id TYPE i
                  client_id TYPE string.
    DATA:
      m_api_key TYPE int2,
      m_api_version TYPE int2,
      m_correlation_id TYPE i,
      m_client_id TYPE string.
ENDCLASS.

CLASS lcl_kafka_request_header IMPLEMENTATION.

   METHOD constructor.
     m_api_key = api_key.
     m_api_version = api_version.
     m_correlation_id = correlation_id.
     m_client_id = client_id.
   ENDMETHOD.

   METHOD lif_kafka_serializable_object~serialize.
     result_xstring = lcl_serialize=>serialize_int2( m_api_key ).
     DATA(tmp_api_version) = lcl_serialize=>serialize_int2( m_api_version ).
     DATA(tmp_correlation_id) = lcl_serialize=>serialize_int( m_correlation_id ).
     DATA(tmp_client_id) = lcl_serialize=>serialize_string( m_client_id ).
     CONCATENATE result_xstring tmp_api_version tmp_correlation_id tmp_client_id INTO result_xstring IN BYTE MODE.
   ENDMETHOD.
ENDCLASS.

CLASS lcl_kafka_array DEFINITION.
  PUBLIC SECTION.
    INTERFACES: lif_kafka_serializable_object.
    DATA      : m_tab_array TYPE tab_kafka_serializable.

ENDCLASS.

CLASS lcl_kafka_array IMPLEMENTATION.
   METHOD lif_kafka_serializable_object~serialize.
     DATA: lv_lines TYPE i.
     DATA: lv_tmp_serial TYPE xstring.
     DESCRIBE TABLE m_tab_array LINES lv_lines.
     result_xstring = lcl_serialize=>serialize_int( lv_lines ).  "put the length at the front
     LOOP AT m_tab_array INTO DATA(ls_kafka_serializable).
       lv_tmp_serial = ls_kafka_serializable->serialize( ).
       CONCATENATE result_xstring lv_tmp_serial INTO result_xstring IN BYTE MODE.
     ENDLOOP.
   ENDMETHOD.
ENDCLASS.


CLASS lcl_kafka_message DEFINITION.
  PUBLIC SECTION.
    INTERFACES: lif_kafka_serializable_object.
    METHODS:
      constructor
        IMPORTING key TYPE string
                  value TYPE string.
    DATA:
      m_magic_byte TYPE int1,
      m_attributes TYPE int1,
      m_timestamp TYPE int8, "cl_pco_utility=>CONVERT_ABAP_TIMESTAMP_TO_JAVA
      m_key TYPE string,
      m_value TYPE string.

ENDCLASS.

CLASS lcl_kafka_message IMPLEMENTATION.

   METHOD constructor.
     m_magic_byte = 1.
     m_attributes = 0.
     m_key = key.
     m_value = value.
   ENDMETHOD.

   METHOD lif_kafka_serializable_object~serialize.
     DATA(tmp_magic_byte) = lcl_serialize=>serialize_int1( m_magic_byte ).
     DATA(tmp_attributes) = lcl_serialize=>serialize_int1( m_attributes ).
     DATA tsl TYPE timestampl.
     GET TIME STAMP FIELD tsl.
     CONVERT TIME STAMP tsl TIME ZONE 'UTC'
        INTO DATE DATA(ts_date)
             TIME DATA(ts_time).
     CALL METHOD CL_PCO_UTILITY=>CONVERT_ABAP_TIMESTAMP_TO_JAVA
       EXPORTING
         IV_DATE      = ts_date
         IV_TIME      = ts_time
       IMPORTING
         EV_TIMESTAMP = DATA(ts_string).
     m_timestamp = ts_string.
     DATA(tmp_timestamp) = lcl_serialize=>serialize_int8( m_timestamp ).
     DATA(tmp_key) = lcl_serialize=>serialize_string_as_bytes( m_key ).
     DATA(tmp_value) = lcl_serialize=>serialize_string_as_bytes( iv_string = m_value ).
     CONCATENATE tmp_magic_byte tmp_attributes tmp_timestamp tmp_key tmp_value INTO result_xstring IN BYTE MODE.

     CALL METHOD CL_ABAP_ZIP=>CRC32
       EXPORTING
         CONTENT =  result_xstring
       RECEIVING
         CRC32   = DATA(lv_crc)
         .
     DATA(tmp_crc) = lcl_serialize=>serialize_int( lv_crc ).

     CONCATENATE tmp_crc result_xstring INTO result_xstring IN BYTE MODE.
   ENDMETHOD.
ENDCLASS.



CLASS lcl_kafka_message_set_item DEFINITION.
  PUBLIC SECTION.
    INTERFACES: lif_kafka_serializable_object.
    METHODS:
      constructor
        IMPORTING offset TYPE int8
                  message TYPE REF TO lcl_kafka_message.
    DATA:
      m_offset TYPE int8,
      m_message TYPE REF TO lcl_kafka_message.

ENDCLASS.

CLASS lcl_kafka_message_set_item IMPLEMENTATION.

   METHOD constructor.
     m_offset = offset.
     m_message = message.
   ENDMETHOD.

   METHOD lif_kafka_serializable_object~serialize.
     result_xstring = lcl_serialize=>serialize_int8( m_offset ).
     DATA(tmp_binary_message) = m_message->lif_kafka_serializable_object~serialize( ).
     DATA(tmp_kafka_msg_size) = lcl_serialize=>serialize_int( XSTRLEN( tmp_binary_message ) ).
     CONCATENATE result_xstring tmp_kafka_msg_size tmp_binary_message INTO result_xstring IN BYTE MODE.
   ENDMETHOD.
ENDCLASS.



TYPES tab_kafka_message_set_item TYPE TABLE OF REF TO lcl_kafka_message_set_item.

CLASS lcl_kafka_message_set DEFINITION.
  PUBLIC SECTION.
    INTERFACES: lif_kafka_serializable_object.
    DATA:
      m_item_tab TYPE tab_kafka_message_set_item.

ENDCLASS.

CLASS lcl_kafka_message_set IMPLEMENTATION.
   METHOD lif_kafka_serializable_object~serialize.
     DATA: lv_tmp_serial TYPE xstring.
     LOOP AT m_item_tab INTO DATA(ls_kafka_serializable).
       lv_tmp_serial = ls_kafka_serializable->lif_kafka_serializable_object~serialize( ).
       CONCATENATE result_xstring lv_tmp_serial INTO result_xstring IN BYTE MODE.
     ENDLOOP.

   ENDMETHOD.
ENDCLASS.



CLASS lcl_kafka_data DEFINITION.
  PUBLIC SECTION.
    INTERFACES: lif_kafka_serializable_object.
    METHODS:
      constructor
        IMPORTING partition TYPE i
                  record_set TYPE REF TO lcl_kafka_message_set.
    DATA:
      m_partition TYPE i,
      m_record_set TYPE REF TO lcl_kafka_message_set.

ENDCLASS.

CLASS lcl_kafka_data IMPLEMENTATION.

   METHOD constructor.
     m_partition = partition.
     m_record_set = record_set.
   ENDMETHOD.

   METHOD lif_kafka_serializable_object~serialize.
     result_xstring = lcl_serialize=>serialize_int( m_partition ).
     DATA(tmp_record_set) = m_record_set->lif_kafka_serializable_object~serialize( ).
     DATA(tmp_record_set_len) = lcl_serialize=>serialize_int( XSTRLEN( tmp_record_set ) ).
     CONCATENATE result_xstring tmp_record_set_len tmp_record_set INTO result_xstring IN BYTE MODE.
   ENDMETHOD.
ENDCLASS.


CLASS lcl_kafka_topic_data DEFINITION.
  PUBLIC SECTION.
    INTERFACES: lif_kafka_serializable_object.
    METHODS:
      constructor
        IMPORTING topic TYPE string.
    METHODS:
      append
        IMPORTING data TYPE REF TO lcl_kafka_data.
    DATA:
      m_topic TYPE string,
      m_data_tab  TYPE REF TO lcl_kafka_array.

ENDCLASS.

CLASS lcl_kafka_topic_data IMPLEMENTATION.

   METHOD constructor.
     m_topic = topic.
     CREATE OBJECT m_data_tab.
   ENDMETHOD.

   METHOD append.
     APPEND DATA to m_data_tab->m_tab_array.
   ENDMETHOD.

   METHOD lif_kafka_serializable_object~serialize.
     result_xstring = lcl_serialize=>serialize_string( m_topic ).
     DATA(tmp_data) = m_data_tab->lif_kafka_serializable_object~serialize( ).
     CONCATENATE result_xstring tmp_data INTO result_xstring IN BYTE MODE.
   ENDMETHOD.
ENDCLASS.



CLASS lcl_kafka_produce_request DEFINITION.
  PUBLIC SECTION.
    INTERFACES: lif_kafka_serializable_object.
    METHODS:
      constructor
        IMPORTING acks TYPE int2
                  timeout TYPE i.
    METHODS:
      append
        IMPORTING topic_data TYPE REF TO lcl_kafka_topic_data.
    DATA:
      m_acks TYPE int2,
      m_timeout TYPE i,
      m_topic_data_tab TYPE REF TO lcl_kafka_array.
ENDCLASS.

CLASS lcl_kafka_produce_request IMPLEMENTATION.

   METHOD constructor.
     m_acks = acks.
     m_timeout = timeout.
     CREATE OBJECT m_topic_data_tab.
   ENDMETHOD.

   METHOD append.
     APPEND topic_data to m_topic_data_tab->m_tab_array.
   ENDMETHOD.



   METHOD lif_kafka_serializable_object~serialize.
     result_xstring = lcl_serialize=>serialize_int2( m_acks ).
     DATA(tmp_timeout) = lcl_serialize=>serialize_int( m_timeout ).
     DATA(tmp_topic_data) = m_topic_data_tab->lif_kafka_serializable_object~serialize( ).
     CONCATENATE result_xstring tmp_timeout tmp_topic_data INTO result_xstring IN BYTE MODE.
   ENDMETHOD.
ENDCLASS.


CLASS lcl_kafka_produce_resp_part DEFINITION.
  PUBLIC SECTION.
    CLASS-METHODS:
      pop_resp_part
        EXPORTING response_partition TYPE REF TO lcl_kafka_produce_resp_part
        CHANGING response_bytes TYPE xstring.
    DATA:
      m_partition_id TYPE i,
      m_error TYPE int2,
      m_offset TYPE int8,
      m_time TYPE int8.
ENDCLASS.

CLASS lcl_kafka_produce_resp_part IMPLEMENTATION.

   METHOD pop_resp_part.
     CREATE OBJECT response_partition.

     CALL METHOD lcl_serialize=>pop_int
       IMPORTING ev_int = response_partition->m_partition_id
       CHANGING iv_bytes = response_bytes.

     CALL METHOD lcl_serialize=>pop_int2
       IMPORTING ev_int2 = response_partition->m_error
       CHANGING iv_bytes = response_bytes.

     CALL METHOD lcl_serialize=>pop_int8
       IMPORTING ev_int8 = response_partition->m_offset
       CHANGING iv_bytes = response_bytes.

     CALL METHOD lcl_serialize=>pop_int8
       IMPORTING ev_int8 = response_partition->m_time
       CHANGING iv_bytes = response_bytes.

   ENDMETHOD.

ENDCLASS.

TYPES tab_kafka_produce_resp_part TYPE TABLE OF REF TO lcl_kafka_produce_resp_part.


CLASS lcl_kafka_produce_resp_topic DEFINITION.
  PUBLIC SECTION.
    CLASS-METHODS:
      pop_response_topic
        EXPORTING response_topic TYPE REF TO lcl_kafka_produce_resp_topic
        CHANGING response_bytes TYPE xstring.
    DATA:
      m_topic TYPE string,
      m_produce_resp_part_count TYPE i.
    DATA:
      m_produce_resp_part_tab TYPE tab_kafka_produce_resp_part.
ENDCLASS.

CLASS lcl_kafka_produce_resp_topic IMPLEMENTATION.

   METHOD pop_response_topic.
     CREATE OBJECT response_topic.

     CALL METHOD lcl_serialize=>pop_string
       IMPORTING ev_string = response_topic->m_topic
       CHANGING iv_bytes = response_bytes.

     CALL METHOD lcl_serialize=>pop_int
       IMPORTING ev_int = response_topic->m_produce_resp_part_count
       CHANGING iv_bytes = response_bytes.

     DATA lv_resp_part TYPE REF TO lcl_kafka_produce_resp_part.

     DO response_topic->m_produce_resp_part_count TIMES.
       CALL METHOD lcl_kafka_produce_resp_part=>pop_resp_part
          IMPORTING response_partition = lv_resp_part
          CHANGING response_bytes = response_bytes.
       APPEND lv_resp_part to response_topic->m_produce_resp_part_tab.
     ENDDO.
   ENDMETHOD.

ENDCLASS.

TYPES tab_kafka_produce_resp_topic TYPE TABLE OF REF TO lcl_kafka_produce_resp_topic.

CLASS lcl_kafka_produce_response DEFINITION.
  PUBLIC SECTION.
    METHODS:
      constructor
        IMPORTING response_bytes TYPE xstring.
    DATA:
      m_length TYPE i,
      m_correlation_id TYPE i,
      m_produce_response_topic_count TYPE i,
      m_produce_response_topics_tab TYPE tab_kafka_produce_resp_topic,
      m_throttle_time_ms TYPE i.
ENDCLASS.

CLASS lcl_kafka_produce_response IMPLEMENTATION.

   METHOD constructor.
     DATA(tmp_xstring) = response_bytes.
     CALL METHOD lcl_serialize=>pop_int
       IMPORTING ev_int = m_length
       CHANGING iv_bytes = tmp_xstring.

     CALL METHOD lcl_serialize=>pop_int
       IMPORTING ev_int = m_correlation_id
       CHANGING iv_bytes = tmp_xstring.

     CALL METHOD lcl_serialize=>pop_int
       IMPORTING ev_int = m_produce_response_topic_count
       CHANGING iv_bytes = tmp_xstring.

     DATA lv_topic TYPE REF TO lcl_kafka_produce_resp_topic.
     DO m_produce_response_topic_count TIMES.
       CALL METHOD lcl_kafka_produce_resp_topic=>pop_response_topic
         IMPORTING response_topic = lv_topic
         CHANGING response_bytes = tmp_xstring.
       APPEND lv_topic to m_produce_response_topics_tab.
     ENDDO.

     CALL METHOD lcl_serialize=>pop_int
       IMPORTING ev_int = m_throttle_time_ms
       CHANGING iv_bytes = tmp_xstring.

   ENDMETHOD.

ENDCLASS.




CLASS lcl_kafka_producer_config DEFINITION.
  PUBLIC SECTION.
    METHODS:
      constructor.
    DATA:
      m_broker_host TYPE string,
      m_broker_port TYPE i,
      m_topic TYPE string,
      m_client_id TYPE string,
      m_acks TYPE int2,
      m_timeout TYPE i.
ENDCLASS.

CLASS lcl_kafka_producer_config IMPLEMENTATION.
  METHOD constructor.
      m_acks = 1.
      m_timeout = 1500.
  ENDMETHOD.
ENDCLASS.

" implementing class for the event handler interface IF_APC_WSP_EVENT_HANDLER
CLASS lcl_apc_handler DEFINITION
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    INTERFACES: if_apc_wsp_event_handler.
    DATA      : m_message TYPE string,
                m_kafka_produce_response TYPE REF TO lcl_kafka_produce_response.
ENDCLASS.

CLASS lcl_apc_handler IMPLEMENTATION.
  METHOD if_apc_wsp_event_handler~on_open.
    DATA s_tst TYPE timestampl.
    GET TIME STAMP FIELD s_tst.
    WRITE: / s_tst TIME ZONE 'UTC',
             ' In APC~on_open'.
  ENDMETHOD.

  METHOD if_apc_wsp_event_handler~on_message.
    "message handling
    TRY.
      DATA(lv_bin_msg) = i_message->get_binary( ).
      CREATE OBJECT m_kafka_produce_response exporting response_bytes = lv_bin_msg.
      DATA: lv_offset TYPE string,
            lv_error TYPE string.
      lv_offset = m_kafka_produce_response->m_produce_response_topics_tab[ 1 ]->m_produce_resp_part_tab[ 1 ]->m_offset.
      lv_error = m_kafka_produce_response->m_produce_response_topics_tab[ 1 ]->m_produce_resp_part_tab[ 1 ]->m_error.
      CONCATENATE 'Kafka Response, Error Code: ' lv_error ', Offset: ' lv_offset INTO m_message.
      CATCH cx_apc_error INTO DATA(lx_apc_error).
        m_message = lx_apc_error->get_text( ).
    ENDTRY.
  ENDMETHOD.

  METHOD if_apc_wsp_event_handler~on_close.
    "close handling
    DATA s_tst TYPE timestampl.
    GET TIME STAMP FIELD s_tst.
    WRITE: / s_tst TIME ZONE 'UTC',
             ' In APC~on_close '.
    m_message = 'Connection closed !'.
  ENDMETHOD.

  METHOD if_apc_wsp_event_handler~on_error.
    "error/close handling
    DATA s_tst TYPE timestampl.
    GET TIME STAMP FIELD s_tst.
    WRITE: / s_tst TIME ZONE 'UTC',
             ' In APC~on_error '.
 ENDMETHOD.
ENDCLASS.



CLASS lcl_kafka_producer DEFINITION.
  PUBLIC SECTION.
    METHODS:
      constructor
        IMPORTING config TYPE REF TO lcl_kafka_producer_config
        RAISING CX_APC_ERROR.
    METHODS:
      send
        IMPORTING
          topic TYPE string
          key TYPE string
          value TYPE string
        RAISING CX_APC_ERROR.
    METHODS:
      close
        RAISING CX_APC_ERROR.

    DATA: m_config TYPE REF TO lcl_kafka_producer_config.
    DATA: m_apc_event_handler   TYPE REF TO lcl_apc_handler.
    DATA: m_apc_client          TYPE REF TO if_apc_wsp_client.
    DATA: m_apc_message_manager TYPE REF TO if_apc_wsp_message_manager.
ENDCLASS.

CLASS lcl_kafka_producer IMPLEMENTATION.

   METHOD constructor.
     DATA: lv_frame           TYPE apc_tcp_frame.
     m_config = config.
      " create the event handler object,  the interface IF_APC_WSP_EVENT_HANDLER is implemented in local class lcl_apc_handler
      CREATE OBJECT m_apc_event_handler.

      " specification of TCP frame
      lv_frame-frame_type = if_apc_tcp_frame_types=>CO_FRAME_TYPE_LENGTH_FIELD. "frames determined by initial header of size bytes
      lv_frame-LENGTH_FIELD_OFFSET = 0. "size is the first
      lv_frame-LENGTH_FIELD_HEADER = 4. "4 byte size at front of message that is the net
      lv_frame-LENGTH_FIELD_LENGTH = 4.
      m_apc_client = cl_apc_tcp_client_manager=>create( i_host = host
                                                     i_port = port
                                                     i_frame = lv_frame
                                                     i_event_handler = m_apc_event_handler ).

      " initiate the connection setup, successful connect leads to execution of ON_OPEN
      DATA s_tst TYPE timestampl.
      GET TIME STAMP FIELD s_tst.
      WRITE: / s_tst TIME ZONE 'UTC',
               ' Opening APC Connection '.
      m_apc_client->connect( ).

      "create message manager and message object for sending the message
      m_apc_message_manager ?= m_apc_client->get_message_manager( ).

   ENDMETHOD.

   METHOD send.
      DATA: lo_message         TYPE REF TO if_apc_wsp_message.
      DATA: lo_k_request_header TYPE REF TO lcl_kafka_request_header.
      DATA: lo_k_msg TYPE REF TO lcl_kafka_message.
      DATA: lo_k_msg_set_item TYPE REF TO lcl_kafka_message_set_item.
      DATA: lo_k_msg_set TYPE REF TO lcl_kafka_message_set.
      DATA: lo_k_data TYPE REF TO lcl_kafka_data.
      DATA: lo_k_topic_data TYPE REF to lcl_kafka_topic_data.
      DATA: lo_k_produce_request TYPE REF to lcl_kafka_produce_request.

             CREATE OBJECT lo_k_msg EXPORTING
            key = key
            value = value.
          CREATE OBJECT lo_k_msg_set_item EXPORTING offset = 0 message = lo_k_msg .
          CREATE OBJECT lo_k_msg_set.
          APPEND lo_k_msg_set_item TO lo_k_msg_set->m_item_tab.
          CREATE OBJECT lo_k_data EXPORTING partition = 0 record_set = lo_k_msg_set.
          CREATE OBJECT lo_k_topic_data EXPORTING topic = topic.
          lo_k_topic_data->append( lo_k_data ).
          CREATE OBJECT lo_k_produce_request
            EXPORTING acks = m_config->m_acks
                      timeout = m_config->m_timeout.
          lo_k_produce_request->append( lo_k_topic_data ).
          CREATE OBJECT lo_k_request_header exporting api_key = 0
                                                 api_version = 2
                                                 correlation_id = 88
                                                 client_id = m_config->m_client_id.

          lo_message ?= m_apc_message_manager->create_message( ).

          DATA(lv_bin_produce_req) = lo_k_produce_request->lif_kafka_serializable_object~serialize( ).
          DATA(lv_bin_req_head) = lo_k_request_header->lif_kafka_serializable_object~serialize( ).
          CONCATENATE lv_bin_req_head lv_bin_produce_req INTO DATA(lv_binary_message) IN BYTE MODE.

          "Add the Length header
          DATA(lv_bin_len) = lcl_serialize=>serialize_int( XSTRLEN( lv_binary_message ) ).
          CONCATENATE lv_bin_len lv_binary_message INTO lv_binary_message IN BYTE MODE.

          lo_message->set_binary( lv_binary_message ).
          DATA lv_total_msg_len TYPE string.
          lv_total_msg_len = XSTRLEN( lv_binary_message ).
          DATA s_tst TYPE timestampl.
          GET TIME STAMP FIELD s_tst.
          WRITE: / s_tst TIME ZONE 'UTC',
                  ' Sending Message of ',
                  lv_total_msg_len,
                  ' bytes'.
          m_apc_message_manager->send( lo_message ).

          "wait for the a message from peer
          CLEAR: m_apc_event_handler->m_message.
          WAIT FOR PUSH CHANNELS UNTIL m_apc_event_handler->m_message IS NOT INITIAL UP TO 10 SECONDS.
          IF sy-subrc = 8.
            WRITE / 'Timeout occured !'.
          ELSE.
            GET TIME STAMP FIELD s_tst.
            WRITE: / s_tst TIME ZONE 'UTC',
                     ' ',
                     m_apc_event_handler->m_message.
          ENDIF.

   ENDMETHOD.

  METHOD close.
      " close connection
      m_apc_client->close( i_reason = 'application closed connection !' ).
  ENDMETHOD.

ENDCLASS.

START-OF-SELECTION.

  DATA: lo_k_p_config TYPE REF TO lcl_kafka_producer_config.
  DATA: lo_k_p TYPE REF TO lcl_kafka_producer.

  TRY.
    CREATE OBJECT lo_k_p_config.
    lo_k_p_config->m_broker_host = host.
    lo_k_p_config->m_broker_port = port.
    lo_k_p_config->m_topic = topic.
    lo_k_p_config->m_client_id = clientid.
    CREATE OBJECT lo_k_p EXPORTING config = lo_k_p_config.

    lo_k_p->send(
      topic = topic
      key = ''
      value = message ).

    lo_k_p->close( ).
  CATCH cx_apc_error INTO DATA(lx_apc_error).
   WRITE / lx_apc_error->get_text( ).
  CATCH cx_root INTO DATA(lx_root).
   WRITE / lx_root->get_text( ).
  ENDTRY.