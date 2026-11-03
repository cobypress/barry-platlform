@isTest
private class BT_AccountHandlerTest {
    @isTest
    static void stampsARegion() {
        Account a = new Account(Name = 'Barometer test 0001');
        insert a;
        System.assertNotEquals(null, [SELECT BT_Region__c FROM Account WHERE Id = :a.Id WITH SYSTEM_MODE].BT_Region__c);
    }
}
