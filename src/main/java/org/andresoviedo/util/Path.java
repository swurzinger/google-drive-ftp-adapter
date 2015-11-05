package org.andresoviedo.util;

public class Path {

	public static final String PATH_SEPARATOR = "/";
	
	
	public static String join(String... path) {
		if (path.length == 0) return "";
		if (path.length == 1) return path[0];
		String p = path[0];
		for (int i = 1; i < path.length; i++) {
			p = join(p, path[i]);
		}
		return p;
	}
	
	public static String join(String a, String b) {
		if (a.isEmpty()) return b;
		if (b.isEmpty()) return a;
		if (a.endsWith(PATH_SEPARATOR)) {
			if (b.startsWith(PATH_SEPARATOR)) {
				return a.substring(0, a.length()-1) + b;
			} else {
				return a+b;
			}
		} else {
			if (b.startsWith(PATH_SEPARATOR)) {
				return a+b;
			} else {
				return a+PATH_SEPARATOR+b;
			}
			
		}
	}
	
	
	public static boolean equals(String a, String b) {
		if (a.equals(b)) return true;
		if (a.length() > 1 && a.endsWith(PATH_SEPARATOR)) a = a.substring(0, a.length()-1);
		if (b.length() > 1 && b.endsWith(PATH_SEPARATOR)) b = b.substring(0, b.length()-1);
		return a.equals(b);
	}
	
	
	
}
