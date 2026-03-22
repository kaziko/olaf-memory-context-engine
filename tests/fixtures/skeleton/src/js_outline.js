/** Base animal class with field, constructor, and method. */
class Animal {
    name = "unknown";

    constructor(name) {
        this.name = name;
    }

    speak() {
        return `${this.name} makes a noise.`;
    }
}

/** Dog extends Animal with an additional method. */
class Dog extends Animal {
    breed = "mixed";

    bark() {
        return `${this.name} barks.`;
    }
}

/** Factory function for creating animals. */
export function createAnimal(name) {
    return new Animal(name);
}
