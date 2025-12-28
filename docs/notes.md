# GMN Adapter Notes

20251228-09:03 -- Before creating a call to GMN to synchronize a data package, the adapter should check to see if a predecessor package exists. If so, and if it too has not yet been synchronized with GMN, then it should be synchronized first. This process shoul be performed recursively so that the appropirate obsolescence chain is maintained.
