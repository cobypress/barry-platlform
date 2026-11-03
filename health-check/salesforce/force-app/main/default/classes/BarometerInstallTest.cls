@IsTest
private class BarometerInstallTest {
    static {
        BarometerInstall.name = 'Barometer_Read_Only_Test';
        BarometerInstall.label = 'Barometer Read Only Test';
    }

    static final List<String> WRITES = new List<String>{
        'PermissionsModifyAllData', 'PermissionsViewAllData', 'PermissionsModifyMetadata', 'PermissionsCustomizeApplication', 'PermissionsAuthorApex', 'PermissionsManageUsers'
    };

    static PermissionSet created() {
        String name = BarometerInstall.name;
        List<String> fields = new List<String>(BarometerInstall.USER_PERMISSIONS);
        fields.addAll(WRITES);
        return Database.query('SELECT Id, ' + String.join(fields, ', ') + ' FROM PermissionSet WHERE Name = :name');
    }

    @IsTest
    static void createsAReadOnlySetOnInstall() {
        Test.testInstall(new BarometerInstall(), null);

        PermissionSet ps = created();
        for (String permission : BarometerInstall.USER_PERMISSIONS) {
            Assert.isTrue((Boolean) ps.get(permission), permission);
        }
        for (String permission : WRITES) {
            Assert.isFalse((Boolean) ps.get(permission), permission);
        }
        List<ObjectPermissions> grants = [
            SELECT SobjectType, PermissionsRead, PermissionsCreate, PermissionsEdit, PermissionsDelete, PermissionsModifyAllRecords
            FROM ObjectPermissions WHERE ParentId = :ps.Id
        ];
        Assert.areEqual(BarometerInstall.OBJECTS.size(), grants.size());
        for (ObjectPermissions op : grants) {
            Assert.isTrue(op.PermissionsRead, op.SobjectType);
            Assert.isFalse(op.PermissionsCreate || op.PermissionsEdit || op.PermissionsDelete || op.PermissionsModifyAllRecords, op.SobjectType);
        }
    }

    @IsTest
    static void leavesAnExistingSetAlone() {
        insert new PermissionSet(Name = BarometerInstall.name, Label = BarometerInstall.label, Description = 'Made by the admin');

        Test.testInstall(new BarometerInstall(), new Version(0, 1));

        PermissionSet ps = [SELECT Description, PermissionsApiEnabled FROM PermissionSet WHERE Name = :BarometerInstall.name];
        Assert.areEqual('Made by the admin', ps.Description);
        Assert.isFalse(ps.PermissionsApiEnabled);
    }
}
