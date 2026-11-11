import '@angular/compiler';
import { TestBed } from '@angular/core/testing';
import { BrowserTestingModule, platformBrowserTesting } from '@angular/platform-browser/testing';
import { afterEach } from 'vitest';

TestBed.initTestEnvironment(BrowserTestingModule, platformBrowserTesting(), {
  teardown: { destroyAfterEach: true },
});

// TestBed resets itself through a global `afterEach` (Jasmine / Jest). Vitest
// globals are off here, so register the reset explicitly.
afterEach(() => TestBed.resetTestingModule());
