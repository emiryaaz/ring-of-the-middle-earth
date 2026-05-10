package rotr.streams;

import java.util.Map;

public class GameUnitRegistry {

    private static final Map<String, UnitStateRecord> UNITS = Map.ofEntries(

            Map.entry("aragorn", create("aragorn", "light", "bree", 0)),
            Map.entry("legolas", create("legolas", "light", "bree", 0)),
            Map.entry("gimli", create("gimli", "light", "bree", 0)),

            Map.entry("test-unit", create("test-unit", "light", "bree", 0)),
            Map.entry("test-unit-2", create("test-unit-2", "light", "bree", 0)),
            Map.entry("test-unit-3", create("test-unit-3", "light", "bree", 0)),
            Map.entry("test-unit-4", create("test-unit-4", "light", "bree", 0)),
            Map.entry("test-unit-5",create("test-unit-5","light","bree",0)),

            Map.entry("gandalf", create("gandalf", "light", "rohan", 2)),

            Map.entry("witch-king", create("witch-king", "dark", "mordor", 0)),
            Map.entry("saruman", create("saruman", "dark", "isengard", 3))
    );

    private static UnitStateRecord create(String unitId, String side, String region, int cooldown) {
        UnitStateRecord unit = new UnitStateRecord();

        unit.unitId = unitId;
        unit.side = side;
        unit.region = region;
        unit.cooldown = cooldown;
        unit.status = "ACTIVE";
        unit.route = new String[0];
	unit.routeIdx = 0;

        return unit;
    }

    public static UnitStateRecord getUnit(String unitId) {
        return UNITS.get(unitId);
    }
}
