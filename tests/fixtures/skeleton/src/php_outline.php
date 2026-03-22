<?php

namespace Outline;

interface Renderable {
    public function render(): string;
    public function getLabel(): string;
}

class Widget implements Renderable {
    public string $name;
    public int $width = 0;

    public function __construct(string $name, int $width) {
        $this->name = $name;
        $this->width = $width;
    }

    public function render(): string {
        return $this->name;
    }

    public function getLabel(): string {
        return 'Widget';
    }
}

function create_widget(string $name): Widget {
    return new Widget($name, 100);
}
