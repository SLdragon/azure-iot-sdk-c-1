// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdio.h>
#include <stdlib.h>

#include "azure_c_shared_utility/xlogging.h"
#include "azure_c_shared_utility/platform.h"
#include "azure_c_shared_utility/threadapi.h"
#include "azure_c_shared_utility/crt_abstractions.h"
#include "azure_c_shared_utility/shared_util_options.h"
#include "iothub_client.h"
#include "iothub_client_options.h"
#include "iothub_message.h"
#include "iothubtransportamqp.h"
#include "iothub_account.h"
#include "iothubtest.h"

#ifdef MBED_BUILD_TIMESTAMP
#define SET_TRUSTED_CERT_IN_SAMPLES
#endif // MBED_BUILD_TIMESTAMP

#ifdef SET_TRUSTED_CERT_IN_SAMPLES
#include "certs.h"
#endif // SET_TRUSTED_CERT_IN_SAMPLES

#define INDEFINITE_TIME ((time_t)-1)

typedef struct bla_tag
{
    time_t time_sent;
    time_t time_received;
} bla;

typedef struct CONNECTION_STATUS_INFO_TAG
{
    time_t time;
    IOTHUB_CLIENT_CONNECTION_STATUS status;
    IOTHUB_CLIENT_CONNECTION_STATUS_REASON reason;
} CONNECTION_STATUS_INFO;

typedef struct IOTHUB_CLIENT_STATISTICS_TAG
{
    SINGLYLINKEDLIST_HANDLE connection_status_history;
} IOTHUB_CLIENT_STATISTICS;

typedef IOTHUB_CLIENT_STATISTICS* IOTHUB_CLIENT_STATISTICS_HANDLE;

static bool destroy_connection_status_info(const void* item, const void* match_context, bool* continue_processing)
{
    free(item);
    *continue_processing = true;
    return true;
}

static void iothub_client_statistics_destroy(IOTHUB_CLIENT_STATISTICS_HANDLE handle)
{
    if (handle != NULL)
    {
        IOTHUB_CLIENT_STATISTICS_HANDLE stats = (IOTHUB_CLIENT_STATISTICS*)handle;

        singlylinkedlist_remove_if(stats->connection_status_history, destroy_connection_status_info, NULL);
        singlylinkedlist_destroy(stats->connection_status_history);

        free(handle);
    }
}

static IOTHUB_CLIENT_STATISTICS_HANDLE iothub_client_statistics_create()
{
    IOTHUB_CLIENT_STATISTICS* stats;

    if ((stats = ((IOTHUB_CLIENT_STATISTICS*)malloc(sizeof(IOTHUB_CLIENT_STATISTICS)))) == NULL)
    {
        LogError("Failed allocating IOTHUB_CLIENT_STATISTICS");
    }
    else if ((stats->connection_status_history = singlylinkedlist_create()) == NULL)
    {
        LogError("Failed creating list for connection status info");
        iothub_client_statistics_destroy(stats);
        stats = NULL;
    }

    return stats;
}

static char* iothub_client_statistics_to_json(IOTHUB_CLIENT_STATISTICS_HANDLE handle)
{
    char* result;

    if (handle == NULL)
    {
        LogError("Invalid argument (handle is NULL)");
        result = NULL;
    }
    else
    {
        IOTHUB_CLIENT_STATISTICS_HANDLE stats = (IOTHUB_CLIENT_STATISTICS*)handle;

    }

    return result;
}

static void iothub_client_statistics_add_connection_status(IOTHUB_CLIENT_STATISTICS_HANDLE handle, IOTHUB_CLIENT_CONNECTION_STATUS status, IOTHUB_CLIENT_CONNECTION_STATUS_REASON reason)
{
    int result;

    if (handle == NULL)
    {
        LogError("Invalid argument (handle is NULL)");
        result = __FAILURE__;
    }
    else
    {
        CONNECTION_STATUS_INFO* conn_status;

        if ((conn_status = (CONNECTION_STATUS_INFO*)malloc(sizeof(CONNECTION_STATUS_INFO))) == NULL)
        {
            LogError("Failed allocating CONNECTION_STATUS_INFO");
            result = __FAILURE__;
        }
        else
        {
            IOTHUB_CLIENT_STATISTICS_HANDLE stats = (IOTHUB_CLIENT_STATISTICS*)handle;

            if (singlylinkedlist_add(stats->connection_status_history, conn_status) != 0)
            {
                LogError("Failed adding CONNECTION_STATUS_INFO");
                result = __FAILURE__;
            }
            else
            {
                conn_status->status = status;
                conn_status->reason = reason;

                if ((conn_status->time = time(NULL)) == INDEFINITE_TIME)
                {
                    LogError("Failed setting the connection status info time");
                }
                
                result = 0;
            }
        }
    }
    
    return result;
}


static IOTHUB_ACCOUNT_INFO_HANDLE g_iothubAcctInfo = NULL;
static IOTHUB_CLIENT_STATISTICS_HANDLE g_iotHubClientStats = NULL;

static void test_platform_deinit()
{
    if (g_iothubAcctInfo != NULL)
    {
        IoTHubAccount_deinit(g_iothubAcctInfo);
        g_iothubAcctInfo = NULL;
    }

    if (g_iotHubClientStats != NULL)
    {
        iothub_client_statistics_destroy(g_iotHubClientStats);
        g_iotHubClientStats = NULL;
    }

    // Need a double deinit
    platform_deinit();
    platform_deinit();
}

static int test_platform_init()
{
    int result;
    
    if (platform_init() != 0)
    {
        result = __FAILURE__;
    }
    else if ((g_iothubAcctInfo = IoTHubAccount_Init()) == NULL)
    {
        LogError("Failed initializing accounts");
        test_platform_deinit();
        result = __FAILURE__;
    }
    else if ((g_iotHubClientStats = iothub_client_statistics_create()) == NULL)
    {
        LogError("Failed initializing statistics");
        test_platform_deinit();
        result = __FAILURE__;
    }
    else
    {
        platform_init();
        result = 0;
    }

    return result;
}

static void connection_status_callback(IOTHUB_CLIENT_CONNECTION_STATUS status, IOTHUB_CLIENT_CONNECTION_STATUS_REASON reason, void* userContextCallback)
{
    IOTHUB_CLIENT_STATISTICS_HANDLE stats_handle = (IOTHUB_CLIENT_STATISTICS_HANDLE)userContextCallback;

    (void)iothub_client_statistics_add_connection_status(stats_handle, status, reason);
}

static IOTHUBMESSAGE_DISPOSITION_RESULT c2d_message_received_callback(IOTHUB_MESSAGE_HANDLE message, void* userContextCallback)
{
    int* counter = (int*)userContextCallback;
    const unsigned char* buffer = NULL;
    size_t size = 0;
    const char* messageId;
    const char* correlationId;
    const char* userDefinedContentType;
    const char* userDefinedContentEncoding;

    // Message properties
    if ((messageId = IoTHubMessage_GetMessageId(message)) == NULL)
    {
        messageId = "<null>";
    }

    if ((correlationId = IoTHubMessage_GetCorrelationId(message)) == NULL)
    {
        correlationId = "<null>";
    }

    if ((userDefinedContentType = IoTHubMessage_GetContentTypeSystemProperty(message)) == NULL)
    {
        userDefinedContentType = "<null>";
    }

    if ((userDefinedContentEncoding = IoTHubMessage_GetContentEncodingSystemProperty(message)) == NULL)
    {
        userDefinedContentEncoding = "<null>";
    }

    // Message content
    IOTHUBMESSAGE_CONTENT_TYPE contentType = IoTHubMessage_GetContentType(message);

    if (contentType == IOTHUBMESSAGE_BYTEARRAY)
    {
        if (IoTHubMessage_GetByteArray(message, &buffer, &size) == IOTHUB_MESSAGE_OK)
        {
            (void)printf("Received Message [%d]\r\n Message ID: %s\r\n Correlation ID: %s\r\n Content-Type: %s\r\n Content-Encoding: %s\r\n BINARY Data: <<<%.*s>>> & Size=%d\r\n",
                *counter, messageId, correlationId, userDefinedContentType, userDefinedContentEncoding, (int)size, buffer, (int)size);
        }
        else
        {
            (void)printf("Failed getting the BINARY body of the message received.\r\n");
        }
    }
    else if (contentType == IOTHUBMESSAGE_STRING)
    {
        if ((buffer = (const unsigned char*)IoTHubMessage_GetString(message)) != NULL && (size = strlen((const char*)buffer)) > 0)
        {
            (void)printf("Received Message [%d]\r\n Message ID: %s\r\n Correlation ID: %s\r\n Content-Type: %s\r\n Content-Encoding: %s\r\n STRING Data: <<<%.*s>>> & Size=%d\r\n",
                *counter, messageId, correlationId, userDefinedContentType, userDefinedContentEncoding, (int)size, buffer, (int)size);

            // If we receive the work 'quit' then we stop running
        }
        else
        {
            (void)printf("Failed getting the STRING body of the message received.\r\n");
        }
    }
    else
    {
        (void)printf("Failed getting the body of the message received (type %i).\r\n", contentType);
    }

    // Retrieve properties from the message
    MAP_HANDLE mapProperties = IoTHubMessage_Properties(message);
    if (mapProperties != NULL)
    {
        const char*const* keys;
        const char*const* values;
        size_t propertyCount = 0;
        if (Map_GetInternals(mapProperties, &keys, &values, &propertyCount) == MAP_OK)
        {
            if (propertyCount > 0)
            {
                size_t index;

                printf(" Message Properties:\r\n");
                for (index = 0; index < propertyCount; index++)
                {
                    printf("\tKey: %s Value: %s\r\n", keys[index], values[index]);
                }
                printf("\r\n");
            }
        }
    }

    if (size == (strlen("quit") * sizeof(char)) && memcmp(buffer, "quit", size) == 0)
    {
        g_continueRunning = false;
    }

    /* Some device specific action code goes here... */
    (*counter)++;
    return IOTHUBMESSAGE_ACCEPTED;
}




static IOTHUB_CLIENT_HANDLE device_client_create_and_connect(IOTHUB_PROVISIONED_DEVICE* deviceToUse, IOTHUB_CLIENT_TRANSPORT_PROVIDER protocol)
{
    IOTHUB_CLIENT_HANDLE iotHubClientHandle;

    if ((iotHubClientHandle = IoTHubClient_CreateFromConnectionString(deviceToUse->connectionString, protocol)) == NULL)
    {
        LogError("Could not create IoTHubClient");
    }
    else if (deviceToUse->howToCreate == IOTHUB_ACCOUNT_AUTH_X509 &&
        (IoTHubClient_SetOption(iotHubClientHandle, OPTION_X509_CERT, deviceToUse->certificate) != IOTHUB_CLIENT_OK ||
         IoTHubClient_SetOption(iotHubClientHandle, OPTION_X509_PRIVATE_KEY, deviceToUse->primaryAuthentication) != IOTHUB_CLIENT_OK))
    {
        LogError("Could not set the device x509 certificate or privateKey");
        IoTHubClient_Destroy(iotHubClientHandle);
        iotHubClientHandle = NULL;
    }
    else
    {
        bool trace = true;
        unsigned int svc2cl_keep_alive_timeout_secs = 120; // service will send pings at 120 x 7/8 = 105 seconds. Higher the value, lesser the frequency of service side pings.
        double cl2svc_keep_alive_send_ratio = 1.0 / 2.0; // Set it to 120 seconds (240 x 1/2 = 120 seconds) for 4 minutes remote idle. 

#ifdef SET_TRUSTED_CERT_IN_SAMPLES
        (void)IoTHubClient_SetOption(iotHubClientHandle, OPTION_TRUSTED_CERT, certificates);
#endif
        (void)IoTHubClient_SetOption(iotHubClientHandle, OPTION_LOG_TRACE, &trace);
        (void)IoTHubClient_SetOption(iotHubClientHandle, OPTION_PRODUCT_INFO, "C-SDK-LongHaul");
        (void)IoTHubClient_SetOption(iotHubClientHandle, OPTION_SERVICE_SIDE_KEEP_ALIVE_FREQ_SECS, &svc2cl_keep_alive_timeout_secs);
        (void)IoTHubClient_SetOption(iotHubClientHandle, OPTION_REMOTE_IDLE_TIMEOUT_RATIO, &cl2svc_keep_alive_send_ratio);

        if (IoTHubClient_SetConnectionStatusCallback(iotHubClientHandle, connection_status_callback, g_iotHubClientStats) != IOTHUB_CLIENT_OK)
        {
            LogError("Failed setting the connection status callback");
            IoTHubClient_Destroy(iotHubClientHandle);
            iotHubClientHandle = NULL;
        }
        else if (IoTHubClient_SetMessageCallback(iotHubClientHandle, c2d_message_received_callback, g_iotHubClientStats) != IOTHUB_CLIENT_OK)
        {
            LogError("Failed to set the cloud-to-device message callback");
            IoTHubClient_Destroy(iotHubClientHandle);
            iotHubClientHandle = NULL;
        }
        else
        {

        }
    }

    return iotHubClientHandle;
}

static int send_one_telemetry_message(IOTHUB_CLIENT_HANDLE iotHubClientHandle, IOTHUB_CLIENT_STATISTICS_HANDLE iotHubClientStatsHandle)
{
    int result;

    sprintf_s(msgText, sizeof(msgText), "{\"deviceId\":\"myFirstDevice\",\"windSpeed\":%.2f}", avgWindSpeed + (rand() % 4 + 2));
    if ((messages[iterator].messageHandle = IoTHubMessage_CreateFromByteArray((const unsigned char*)msgText, strlen(msgText))) == NULL)
    {
        (void)printf("ERROR: iotHubMessageHandle is NULL!\r\n");
    }
    else
    {
        messages[iterator].messageTrackingId = iterator;


        if (IoTHubClient_SendEventAsync(iotHubClientHandle, messages[iterator].messageHandle, SendConfirmationCallback, &messages[iterator]) != IOTHUB_CLIENT_OK)
        {
            LogError("Failed sending telemetry message");
            result = __FAILURE__;
        }
        else
        {
            result = 0;
        }
    }

    return result;
}

static int verify_telemetry_messages_received()
{
    int result;

    return result;
}

static int callbackCounter;
static bool g_continueRunning;
static char msgText[1024];
static char propText[1024];
#define MESSAGE_COUNT       5
#define DOWORK_LOOP_NUM     3

typedef struct EVENT_INSTANCE_TAG
{
    IOTHUB_MESSAGE_HANDLE messageHandle;
    size_t messageTrackingId;  // For tracking the messages within the user callback.
} EVENT_INSTANCE;

static void SendConfirmationCallback(IOTHUB_CLIENT_CONFIRMATION_RESULT result, void* userContextCallback)
{
    EVENT_INSTANCE* eventInstance = (EVENT_INSTANCE*)userContextCallback;
    (void)printf("Confirmation[%d] received for message tracking id = %zu with result = %s\r\n", callbackCounter, eventInstance->messageTrackingId, ENUM_TO_STRING(IOTHUB_CLIENT_CONFIRMATION_RESULT, result));
    /* Some device specific action code goes here... */
    callbackCounter++;
    IoTHubMessage_Destroy(eventInstance->messageHandle);
}

static int longhaul_amqp_telemetry_run(void)
{
    int result;
    IOTHUB_CLIENT_HANDLE iotHubClientHandle;
    double test_duration_in_seconds = 12 * 60 * 60;
    size_t test_loop_wait_time_in_seconds = 60;

    if (test_platform_init() != 0)
    {
        LogError("Test failed");
        result = __FAILURE__;
    }
    else
    {
        if ((iotHubClientHandle = device_client_create_and_connect(IoTHubAccount_GetSASDevice(g_iothubAcctInfo), AMQP_Protocol)) == NULL)
        {
            LogError("Failed creating the device client");
            result = __FAILURE__;
        }
        else
        {
            time_t test_start_time;

            if ((test_start_time = time(NULL)) == INDEFINITE_TIME)
            {
                LogError("Failed creating the device client");
                result = __FAILURE__;
            }
            else
            {
                time_t test_current_time;

                do
                {
                    if (send_one_telemetry_message(iotHubClientHandle, g_iotHubClientStats) != 0)
                    {
                        LogError("Failed getting the current time");
                        result = __FAILURE__;
                        break;
                    }
                    else
                    {
                        ThreadAPI_Sleep(test_loop_wait_time_in_seconds);

                        if ((test_current_time = time(NULL)) == INDEFINITE_TIME)
                        {
                            LogError("Failed getting the current time");
                            result = __FAILURE__;
                            break;
                        }
                    }
                } while (difftime(test_current_time, test_start_time) < test_duration_in_seconds);
            }

            result = 0;

            IoTHubClient_Destroy(iotHubClientHandle);
        }

        test_platform_deinit();
    }

    return result;
}

int main(void)
{
    return longhaul_amqp_telemetry_run();
}
