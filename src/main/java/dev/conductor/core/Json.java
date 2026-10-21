package dev.conductor.core;

import java.util.*;

/** Small, strict JSON codec; no polymorphic deserialization or runtime dependencies. */
public final class Json {
    private Json() {}
    public static Object parse(String text) {
        Parser p = new Parser(text);
        Object value = p.value(0);
        p.ws();
        if (p.i != text.length()) throw new IllegalArgumentException("Trailing JSON");
        return value;
    }
    @SuppressWarnings("unchecked")
    public static Map<String,Object> obj(Object x) {
        if (!(x instanceof Map<?,?>)) throw new IllegalArgumentException("Expected object");
        return (Map<String,Object>) x;
    }
    @SuppressWarnings("unchecked")
    public static List<Object> arr(Object x) {
        if (!(x instanceof List<?>)) throw new IllegalArgumentException("Expected array");
        return (List<Object>) x;
    }
    public static String str(Map<String,Object> m, String k, String fallback) {
        Object v=m.get(k);
        if(v==null) return fallback;
        if(!(v instanceof String s)) throw new IllegalArgumentException(k+" must be a string");
        return s;
    }
    public static long num(Map<String,Object> m,String k,long fallback) {
        Object v=m.get(k);
        if(v==null) return fallback;
        if(!(v instanceof Number n) || n.doubleValue()!=n.longValue())
            throw new IllegalArgumentException(k+" must be an integer");
        return n.longValue();
    }
    public static String write(Object x) {
        StringBuilder b=new StringBuilder(512);
        writeInto(x,b);
        return b.toString();
    }
    private static void writeInto(Object x,StringBuilder b) {
        if(x==null){b.append("null");return;}
        if(x instanceof String s) {
            b.append('"');
            for(int i=0;i<s.length();i++) {char c=s.charAt(i);switch(c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> { if(c<32) b.append("\\u00").append("0123456789abcdef".charAt(c>>4)).append("0123456789abcdef".charAt(c&15)); else b.append(c); }
            }}
            b.append('"');return;
        }
        if(x instanceof Boolean || x instanceof Number){b.append(x);return;}
        if(x instanceof Map<?,?> m) {
            b.append('{');boolean first=true;
            for(var e:m.entrySet()) {
                if(!first)b.append(',');first=false;
                writeInto(e.getKey().toString(),b);b.append(':');writeInto(e.getValue(),b);
            }
            b.append('}');return;
        }
        if(x instanceof Iterable<?> a) {
            b.append('[');boolean first=true;
            for(Object v:a){if(!first)b.append(',');first=false;writeInto(v,b);}
            b.append(']');return;
        }
        throw new IllegalArgumentException("Unsupported JSON type");
    }
    public static Map<String,Object> copy(Map<String,Object> m) { return obj(deepCopy(m)); }
    public static Object deepCopy(Object x) {return deepCopy(x,0);}
    private static Object deepCopy(Object x,int depth) {
        if(depth>64)throw new IllegalArgumentException("JSON nesting exceeds 64");
        if(x==null||x instanceof String||x instanceof Boolean)return x;
        if(x instanceof Byte||x instanceof Short||x instanceof Integer||x instanceof Long)return ((Number)x).longValue();
        if(x instanceof Float||x instanceof Double) {
            double n=((Number)x).doubleValue();
            if(!Double.isFinite(n))throw new IllegalArgumentException("Invalid number");
            return n;
        }
        if(x instanceof Map<?,?> m) {
            Map<String,Object> copy=new LinkedHashMap<>(m.size());
            for(var e:m.entrySet()) {
                if(!(e.getKey() instanceof String key))throw new IllegalArgumentException("JSON keys must be strings");
                copy.put(key,deepCopy(e.getValue(),depth+1));
            }
            return copy;
        }
        if(x instanceof Iterable<?> a) {
            List<Object> copy=new ArrayList<>();
            for(Object value:a)copy.add(deepCopy(value,depth+1));
            return copy;
        }
        throw new IllegalArgumentException("Unsupported JSON type");
    }
    private static final class Parser {
        final String s; int i;
        Parser(String s){this.s=s;}
        void ws(){while(i<s.length() && " \n\r\t".indexOf(s.charAt(i))>=0)i++;}
        char take(){if(i>=s.length())throw new IllegalArgumentException("Incomplete JSON");return s.charAt(i++);}
        Object value(int depth) {
            if(depth>64) throw new IllegalArgumentException("JSON nesting exceeds 64");
            ws(); char c=take();
            if(c=='"')return string();
            if(c=='{' || c=='[') {
                boolean object=c=='{'; char end=object?'}':']';
                Map<String,Object> m=new LinkedHashMap<>(); List<Object> a=new ArrayList<>();
                ws(); if(i<s.length() && s.charAt(i)==end){i++;return object?m:a;}
                while(true) {
                    if(object) {
                        ws(); if(take()!='"')throw new IllegalArgumentException("Expected key");
                        String k=string(); ws(); if(take()!=':')throw new IllegalArgumentException("Expected colon");
                        if(m.containsKey(k))throw new IllegalArgumentException("Duplicate key");
                        m.put(k,value(depth+1));
                    } else a.add(value(depth+1));
                    ws(); char sep=take(); if(sep==end)break;
                    if(sep!=',')throw new IllegalArgumentException("Expected comma");
                }
                return object?m:a;
            }
            for(String token:List.of("true","false","null")) {
                if(c==token.charAt(0) && s.startsWith(token,i-1)) {
                    i+=token.length()-1;
                    return token.equals("null")?null:token.equals("true");
                }
            }
            int start=i-1;
            while(i<s.length() && "0123456789.eE+-".indexOf(s.charAt(i))>=0)i++;
            String n=s.substring(start,i);
            if(!n.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?"))
                throw new IllegalArgumentException("Invalid JSON value");
            try {
                if(!n.contains(".") && !n.contains("e") && !n.contains("E"))return Long.parseLong(n);
                double d=Double.parseDouble(n);
                if(!Double.isFinite(d))throw new NumberFormatException();
                return d;
            } catch(NumberFormatException e){throw new IllegalArgumentException("Invalid number");}
        }
        String string() {
            StringBuilder b=new StringBuilder();
            while(true) {
                char c=take(); if(c=='"')return b.toString();
                if(c<32)throw new IllegalArgumentException("Control character");
                if(c!='\\'){b.append(c);continue;}
                char e=take();
                switch(e) {
                    case '"','\\','/' -> b.append(e);
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'n' -> b.append('\n');
                    case 'r' -> b.append('\r');
                    case 't' -> b.append('\t');
                    case 'u' -> { try { b.append((char)Integer.parseInt(s.substring(i,i+4),16)); i+=4; }
                                  catch(RuntimeException ex){throw new IllegalArgumentException("Invalid escape");} }
                    default -> throw new IllegalArgumentException("Invalid escape");
                }
            }
        }
    }
}
