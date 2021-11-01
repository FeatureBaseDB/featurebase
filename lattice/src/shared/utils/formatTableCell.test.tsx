import { formatTableCell } from "./formatTableCell";
import React from "react";
import { render, unmountComponentAtNode } from "react-dom";
import { act } from "react-dom/test-utils";

let container = null;
beforeEach(() => {
  // setup a DOM element as a render target
  container = document.createElement("div");
  document.body.appendChild(container);
});

afterEach(() => {
  // cleanup on exiting
  unmountComponentAtNode(container);
  container.remove();
  container = null;
});

it("renders strings in quotes", () => {
  let row = { thing: "quoted string!" };
  let col = { name: "thing", datatype: "[]string" };

  act(() => {
    render(formatTableCell(row, col), container);
  });
  expect(container.textContent).toBe('"quoted string!"');
});

it("renders objects as stringified", () => {
  let row = { thing: { val: "quoted string!" } };
  let col = { name: "thing", datatype: "object" };

  act(() => {
    render(formatTableCell(row, col), container);
  });
  expect(container.textContent).toBe(`{
  "val": "quoted string!"
}`);
});

it("puts timestamps in MM/DD/YYYY hh:mm:ss a format", () => {
  let row = { thing: 1635452050094 };
  let col = { name: "thing", datatype: "timestamp" };

  act(() => {
    render(formatTableCell(row, col), container);
  });
  expect(container.textContent).toBe("10/28/2021 08:14:10 pm");
});

it("renders timestamps with no value as LocaleString", () => {
  let row = { thing: false };
  let col = { name: "thing", datatype: "timestamp" };

  act(() => {
    render(formatTableCell(row, col), container);
  });
  expect(container.textContent).toBe(row.thing.toLocaleString());
});

it("renders non-timestamp, non-string, non-objects as LocaleString", () => {
  let row = { thing: "idk" };
  let col = { name: "thing", datatype: "idk" };

  act(() => {
    render(formatTableCell(row, col), container);
  });
  expect(container.textContent).toBe(row.thing.toLocaleString());
});

it("returns null for undefined objects", () => {
  let row = {};
  let col = { name: "thing", datatype: "" };

  expect(formatTableCell(row, col)).toBeNull();
});
