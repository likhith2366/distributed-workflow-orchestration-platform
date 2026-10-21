package dev.conductor;

/** Test access to package-private startup checks. */
public final class ConductorApplicationAccess {
    private ConductorApplicationAccess() {}
    public static boolean warn(String version){return ConductorApplication.warnOnAffectedJdk(Runtime.Version.parse(version));}
}
