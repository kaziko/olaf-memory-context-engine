/** Interface with properties and method signatures. */
interface Displayable {
    text: string;
    render(): string;
    update(value: string): void;
}

/** Class extending a base and implementing the interface. */
class Greeter extends BaseGreeter implements Displayable {
    message: string;
    count: number;

    constructor(message: string) {
        super();
        this.message = message;
        this.count = 0;
    }

    greet(name: string): string {
        return `${this.message}, ${name}`;
    }

    render(): string {
        return this.message;
    }

    update(value: string): void {
        this.message = value;
    }

    get text(): string {
        return this.message;
    }
}

/** Regular enum with string-valued members. */
enum Status {
    Active = "active",
    Inactive = "inactive",
    Pending = "pending",
}

/** Const enum with numeric members. */
const enum Direction {
    Up = 0,
    Down = 1,
    Left = 2,
    Right = 3,
}

/** Type alias as a union type — must appear with target in signature. */
type Result<T> = Success<T> | Failure;

/** Standalone exported function. */
export function createGreeter(msg: string): Greeter {
    return new Greeter(msg);
}
