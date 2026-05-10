package rotr.streams;

import java.util.Map;
import java.util.Set;

public class PathRegistry {

    private static final Map<String, Set<String>> PATH_ENDPOINTS = Map.ofEntries(
            Map.entry("shire-to-bree", Set.of("the-shire", "bree")),
            Map.entry("bree-to-weathertop", Set.of("bree", "weathertop")),
            Map.entry("bree-to-rivendell", Set.of("bree", "rivendell")),
            Map.entry("rivendell-to-moria", Set.of("rivendell", "moria")),
            Map.entry("bree-to-tharbad", Set.of("bree", "tharbad")),
            Map.entry("shire-to-tharbad", Set.of("the-shire", "tharbad")),

            Map.entry("fords-of-isen-to-edoras", Set.of("fords-of-isen", "edoras")),
            Map.entry("edoras-to-minas-tirith", Set.of("edoras", "minas-tirith")),
            Map.entry("osgiliath-to-minas-morgul", Set.of("osgiliath", "minas-morgul")),
            Map.entry("mordor-to-mount-doom", Set.of("mordor", "mount-doom"))
    );

    private PathRegistry() {
    }

    public static String getOtherEndpoint(String pathId, String currentRegion) {
    Set<String> endpoints = PATH_ENDPOINTS.get(pathId);

    if (endpoints == null || currentRegion == null || !endpoints.contains(currentRegion)) {
        return null;
    }

    return endpoints.stream()
            .filter(region -> !region.equals(currentRegion))
            .findFirst()
            .orElse(null);
}

    public static boolean exists(String pathId) {
        return PATH_ENDPOINTS.containsKey(pathId);
    }

    public static boolean isEndpoint(String pathId, String regionId) {
        Set<String> endpoints = PATH_ENDPOINTS.get(pathId);
        return endpoints != null && endpoints.contains(regionId);
    }
}	

