/**
 * Creates Barometer Read Only when the customer's admin installs the package.
 *
 * A managed package can't carry user permissions or standard object
 * permissions: the install drops them without a warning (DECISIONS.md 67).
 * Barometer Read Only is made of nothing else, so it's created here instead,
 * as an ordinary permission set the admin can inspect. Nothing else in the
 * org is changed, and the health check itself never writes.
 */
global without sharing class BarometerInstall implements InstallHandler {
    // Tests use their own: permission sets are visible to tests, and an org may already have this one.
    @TestVisible static String name = 'Barometer_Read_Only';
    @TestVisible static String label = 'Barometer Read Only';

    // Read only. Matches salesforce/dev-only/main/default/permissionsets (DECISIONS.md 59).
    // In dependency order: View Setup needs View Roles, and View Health Check needs View Setup.
    @TestVisible static final List<String> USER_PERMISSIONS = new List<String>{
        'PermissionsApiEnabled',
        'PermissionsViewRoles',
        'PermissionsViewSetup',
        'PermissionsViewAllUsers',
        'PermissionsMonitorLoginHistory',
        'PermissionsViewHealthCheck',
        'PermissionsViewPublicReports',
        'PermissionsViewPublicDashboards',
        'PermissionsRunReports',
        'PermissionsActivitiesAccess'
    };
    @TestVisible static final List<String> OBJECTS = new List<String>{ 'Account', 'Campaign', 'Case', 'Contact', 'Lead', 'Opportunity' };
    @TestVisible static final List<String> FIELDS = new List<String>{ 'Task.WhatId', 'Task.WhoId' };

    global void onInstall(InstallContext context) {
        // A failure here would fail the whole install, and the installer would see
        // only "the post install script failed". The connect-time check finds a
        // user without what they need, so the install always goes ahead.
        try {
            create();
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Barometer Read Only not created: ' + e.getMessage());
        }
    }

    // Every query and write is in system mode. The script runs as a special user
    // that represents the package and holds almost no permissions, and API 68.0
    // Apex runs database operations in user mode by default: without this the
    // install creates nothing (DECISIONS.md 67).
    @TestVisible static void create() {
        // An existing set is left alone: the admin may have changed it on purpose.
        // Labels must be unique too, so one with our label counts as existing.
        if (![SELECT Id FROM PermissionSet WHERE Name = :name OR Label = :label WITH SYSTEM_MODE].isEmpty()) return;

        String description = 'Black Cloud Barometer health check: read only. Created by the Black Cloud Barometer package.';
        PermissionSet ps = new PermissionSet(Name = name, Label = label, Description = description);
        insert as system ps;

        // One at a time, so an edition without a permission still gets the rest.
        List<String> notSet = new List<String>();
        for (String permission : USER_PERMISSIONS) {
            try {
                PermissionSet one = new PermissionSet(Id = ps.Id);
                one.put(permission, true);
                update as system one;
            } catch (DmlException e) {
                notSet.add(permission.removeStart('Permissions') + ' (' + e.getDmlType(0) + ')');
            }
        }

        List<ObjectPermissions> objectGrants = new List<ObjectPermissions>();
        for (String obj : OBJECTS) {
            objectGrants.add(new ObjectPermissions(
                ParentId = ps.Id,
                SobjectType = obj,
                PermissionsRead = true,
                PermissionsViewAllRecords = true,
                PermissionsViewAllFields = true
            ));
        }
        List<Database.SaveResult> results = Database.insert(objectGrants, false, AccessLevel.SYSTEM_MODE);
        for (Integer i = 0; i < results.size(); i++) {
            if (!results[i].isSuccess()) notSet.add(OBJECTS[i] + ' (' + results[i].getErrors()[0].getStatusCode() + ')');
        }

        List<FieldPermissions> fieldGrants = new List<FieldPermissions>();
        for (String field : FIELDS) {
            fieldGrants.add(new FieldPermissions(ParentId = ps.Id, SobjectType = field.substringBefore('.'), Field = field, PermissionsRead = true));
        }
        results = Database.insert(fieldGrants, false, AccessLevel.SYSTEM_MODE);
        for (Integer i = 0; i < results.size(); i++) {
            if (!results[i].isSuccess()) notSet.add(FIELDS[i] + ' (' + results[i].getErrors()[0].getStatusCode() + ')');
        }

        // Shown to the admin on the permission set, and read back by us when testing installs.
        if (!notSet.isEmpty()) {
            update as system new PermissionSet(Id = ps.Id, Description = (description + ' Not granted: ' + String.join(notSet, ', ')).abbreviate(255));
        }
    }
}
