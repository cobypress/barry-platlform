trigger BT_AccountTrigger on Account (before insert) {
    BT_AccountHandler.stamp(Trigger.new);
}
