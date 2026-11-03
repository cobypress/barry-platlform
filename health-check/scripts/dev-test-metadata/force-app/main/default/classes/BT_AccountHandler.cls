// Barometer test metadata: gives an Account a region when it has none.
public with sharing class BT_AccountHandler {
    public static void stamp(List<Account> accounts) {
        for (Account a : accounts) {
            if (a.BT_Region__c == null) {
                a.BT_Region__c = 'Unknown';
            }
        }
    }
}
