(ns clj-spark.macros
  (:refer-clojure :exclude [for])
  (:require [clj-spark.api :as api]))


;; The syntax looks like this
;;     (for [x (range 1 100)
;;           :when (even? x)
;;           y (range 1 100)
;;           :let [z (* x y)]
;;           :while (< z 900)]
;;          (+ z 1))
;;
;; For a simple form we just want a map:
;; (for [x (range 1 100)] x)
;; =>
;; (let [xs (range 1 100)]
;;   (map (fn [x] x) xs))
;;
;; If there are nested loops, we want a mapcat:
;; (for [x (range 1 100) y (range x 100)] (* x y))
;; =>
;; (let [xs (range 1 100)]
;;   (mapcat (fn [x] (let [ys (range x 100)]
;;                     (map (fn [y] (* x y))))) xs))
;;
;; The signature for flatMap is
;;     RDD a -> (a -> Iterable b) -> RDD b
;; and for map,
;;     RDD a -> (a -> b) -> RDD b
;;
;; meaning you can't just nest a Spark map inside a Spark flatMap; we
;; have to be aware of the context, and only the outermost (flat)map
;; is an RDD operation. One way of avoiding this might be to rewrite
;; as flatMaps that accumulate the arguments used in the inner
;; expressions; i.e., the inversion of the usual comprehension syntax.
;;
;; Here's the naïve implementation, without modifiers.
(defmacro for
  [seq-exprs body]
  (letfn [(to-groups [seq-exprs]
            ;; Group the modifiers with the generator expressions,
            ;; snippet nicked from clojure.core/for
            (reduce (fn [groups [k v]]
                      (if (keyword? k)
                        (conj (pop groups) (conj (peek groups) [k v]))
                        (conj groups [k v])))
                    [] (partition 2 seq-exprs)))

          (emit-group [group gs outermost?]
            (let [v (first group)
                  g (second group)
                  mods (drop 2 group)]
              (if (empty? gs)
                (let [f `(fn [~v] ~body)
                      vs `vs#]
                  `(let [~vs ~g]
                     ~(if outermost? ;; NB different arg order.
                        `(api/map ~vs ~f)
                        `(clojure.core/map ~f ~vs))))
                (let [f `(fn [~v]
                           ~(emit-group (first gs) (rest gs) false))
                      vs `vs#]
                  `(let [~vs ~g]
                     ~(if outermost?
                        `(api/flat-map ~vs ~f)
                        `(mapcat ~f ~vs)))))))]
    (let [groups (to-groups seq-exprs)]
      (emit-group (first groups) (rest groups) true))))
