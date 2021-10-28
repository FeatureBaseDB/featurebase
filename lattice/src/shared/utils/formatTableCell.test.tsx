import { formatTableCell } from './formatTableCell';
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

it("it renders strings in quotes", () => {
  let row = {thing:"quoted string!"};
  let col = {name: "thing", datatype: "[]string"};
    // this is just so we can actually test it, the value doesn't matter that much 
  let css = {preFormat: "preFormat"};

  act(() => {
    render(formatTableCell(row, col, css), container);
  });
  expect(container.textContent).toBe('"quoted string!"');
});

it("renders objects as stringified", () => {
  let row = {thing:{val:"quoted string!"}};
  let col = {name: "thing", datatype: "object"};
    // this is just so we can actually test it, the value doesn't matter that much 
  let css = {preFormat: "preFormat"};

  act(() => {
    render(formatTableCell(row, col, css), container);
  });
  expect(container.textContent).toBe(`{
  "val": "quoted string!"
}`);
});

it("it puts timestamps in MM/DD/YYYY hh:mm:ss a format", () => {
  let row = {thing:1635452050094};
  let col = {name: "thing", datatype: "timestamp"};
    // this is just so we can actually test it, the value doesn't matter that much 
  let css = {preFormat: "preFormat"};

  act(() => {
    render(formatTableCell(row, col, css), container);
  });
  expect(container.textContent).toBe("10/28/2021 08:14:10 pm");
});


