package ohua.loader;

import ohua.StatefulFunctionProvider;
import ohua.runtime.lang.operator.StatefulFunction;
import java.nio.file.*;
import java.net.URL;
import java.net.URI;
import java.util.*;
import java.util.stream.*;
import java.util.function.Consumer;
import ohua.lang.defsfn;
import java.lang.reflect.Method;
import java.util.function.Function;
import ohua.runtime.engine.exceptions.*;
import java.io.IOException;
import java.net.URISyntaxException;
import ohua.util.*;

public final class JavaProviderFromAnnotatedMethod implements StatefulFunctionProvider {
    private Map<String,Optional<Map<String,Method>>> nsMap = new HashMap<>();

    public JavaProviderFromAnnotatedMethod() {}

    @Override
    public boolean exists(String nsRef, String sfRef) {
        return findMethod(nsRef, sfRef) != null;
    }

    @Override
    public StatefulFunction provide(String nsRef, String sfRef) throws Exception {
        Method m = findMethod(nsRef, sfRef);
        if (m == null) return null;
        return StatefulFunction.resolve(
            StatefulFunction.createStatefulFunctionObject(
                m.getDeclaringClass()
            )
        );
    }

    @Override
    public Iterator<String> list(String nsRef) {
        return getOrLoadNS(nsRef).map(Map::keySet).map(Iterable::iterator).orElse(EmptyIterator.it());
    }

    private Optional<Map<String,Method>> getOrLoadNS(String nsRef) {
        if (!nsMap.containsKey(nsRef)) {
            Optional<Map<String,Method>> mmap = tryLoadNS(nsRef);
            nsMap.put(nsRef, mmap);
            return mmap;
        } else {
            return nsMap.get(nsRef);
        }   
    }

    private Optional<Map<String,Method>> tryLoadNS(String nsRef) {
        Optional<Map<String,Method>> m1;
        try {
            m1 = Optional.of(loadNamespace(nsRef));
        } catch (IOException e) {
            m1 = Optional.empty();
        }
        return m1;
    }

    private Method findMethod(String nsRef, String sfRef) {
        return getOrLoadNS(nsRef).map(m -> m.get(sfRef)).orElse(null);
    }

    private Map<String,Method> loadNamespace(String namespace) throws IOException {
        /**
        I left this bit of code out. We need to see wether we need it.
        java-pkg-name (string/replace pkg-name "-" "_")
        */
        String javaPkgName = namespace.replace("-", "_");
        Map<String,Method> mappedToClass =
            loadFromClassPath(namespace.replace(".", "/"), "*.class")
                .map(path -> javaPkgName + "." + path.getFileName().toString().replace(".class", ""))
                .map(className -> {
                        Method m;
                        try {
                            m = getSfnMethod(Class.forName(className));
                        }
                        catch (ClassNotFoundException e) {
                            m = null;
                        }
                        return new Tuple<>(className, m);
                    }
                )
                .filter(t -> t.second() != null)
                .collect(Collectors.toMap(
                    Tuple::first,
                    Tuple::second,
                    (a, b) -> a
                ));
        return mappedToClass.values().stream()
                    .collect(Collectors.toMap(
                        Method::getName,
                        Function.identity(),
                        (a, b) -> {
                            throw new RuntimeException("Stateful function " + a.getName() + " is defined twice.");
                        }
                    ));
    }

    private Method getSfnMethod(Class<?> clazz) {
        List<Method> l = Stream.of(clazz.getDeclaredMethods()).filter(m -> m.isAnnotationPresent(defsfn.class)).collect(Collectors.toList());
        switch (l.size()) {
            case 0: return null;
            case 1: return l.get(0);
            default:
                throw new RuntimeException("Class " + clazz + " defines more than one stateful functions!");
        }
    }

    private Stream<Path> loadFromClassPath(String folder, String glob) throws IOException {
        List<Path> pl = new ArrayList<>();

        for (URL url : Collections.list(Thread.currentThread().getContextClassLoader().getResources(folder))) {

            try {
                String[] fs_and_file = url.toURI().toString().split("!");
                URI uri = URI.create(fs_and_file[0]);
                List<Path> l;
                if (fs_and_file.length == 2) {
                    l = findUrls(getFs(uri), fs_and_file[1], glob);
                } else {
                    l = findUrls(FileSystems.getDefault(), uri.getPath(), glob);
                }
                pl.addAll(l);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        return pl.stream();
    }

    private FileSystem getFs(URI uri) throws IOException {
        try {
            return FileSystems.getFileSystem(uri);
        } catch (FileSystemNotFoundException e) {
            return FileSystems.newFileSystem(uri, new HashMap<>());
        }
    }

    private List<Path> findUrls(FileSystem fs, String folder, String glob) throws IOException {
        List<Path> res = new ArrayList<>();
        for (Path p : Files.newDirectoryStream(fs.getPath(folder, new String[]{}), glob))
            res.add(p);
        return res;

    }
}
