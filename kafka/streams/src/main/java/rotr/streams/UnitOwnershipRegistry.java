package rotr.streams;

import java.util.Map;

public class UnitOwnershipRegistry {

    private static final Map<String, String> UNIT_OWNERS = Map.ofEntries(
            Map.entry("ring-bearer", "light"),
            Map.entry("aragorn", "light"),
            Map.entry("legolas", "light"),
            Map.entry("gimli", "light"),
            Map.entry("rohan-cavalry", "light"),
            Map.entry("gondor-army", "light"),
            Map.entry("gandalf", "light"),

            Map.entry("witch-king", "dark"),
            Map.entry("nazgul-2", "dark"),
            Map.entry("nazgul-3", "dark"),
            Map.entry("uruk-hai-legion", "dark"),
            Map.entry("saruman", "dark"),
            Map.entry("sauron", "dark")
    );

    private UnitOwnershipRegistry() {
    }

    public static String getOwnerSide(String unitId) {
        return UNIT_OWNERS.get(unitId);
    }
}
