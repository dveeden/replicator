package com.booking.replication.binlog;

import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.google.code.or.binlog.StatusVariable;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.variable.status.QTimeZoneCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by bosko on 5/22/17.
 */
public class RawBinlogEvent_Query extends RawBinlogEvent {
    public RawBinlogEvent_Query(Object event) throws Exception {
        super(event);
    }

    public String getSql() {
        if (USING_DEPRECATED_PARSER) {
            return ((QueryEvent) this.getBinlogEventV4()).getSql().toString();
        }
        else {
            return ((QueryEventData) this.getBinlogConnectorEvent().getData()).getSql();
        }
    }

    public String getDatabaseName() {
        if (USING_DEPRECATED_PARSER) {
            return ((QueryEvent) this.getBinlogEventV4()).getDatabaseName().toString();
        }
        else {
            return ((QueryEventData) this.getBinlogConnectorEvent().getData()).getDatabase();
        }
    }

    public boolean hasTimezoneOverride() {
        if (USING_DEPRECATED_PARSER) {
            for (StatusVariable av : ((QueryEvent) this.getBinlogEventV4()).getStatusVariables()) {
                // handle timezone overrides during schema changes
                if (av instanceof QTimeZoneCode) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        else {
            return ((EventHeaderV4) getBinlogConnectorEvent().getHeader()).getFlags();
        }
    }

    public HashMap<String,String> getTimezoneOverrideCommands() {
        if (USING_DEPRECATED_PARSER) {

            HashMap<String,String> sqlCommands = new HashMap<>();

            for (StatusVariable av : ((QueryEvent) this.getBinlogEventV4()).getStatusVariables()) {

                QTimeZoneCode tzCode = (QTimeZoneCode) av;

                String timezone = tzCode.getTimeZone().toString();
                String timezoneSetCommand = "SET @@session.time_zone='" + timezone + "'";
                String timezoneSetBackToSystem = "SET @@session.time_zone='SYSTEM'";

                sqlCommands.put("timezonePre", timezoneSetCommand);
                sqlCommands.put("timezonePost", timezoneSetBackToSystem);
            }

            return sqlCommands;
        }
        else {
            // TODO:
        }
    }
}
