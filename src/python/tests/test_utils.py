# -*- coding: utf-8 -*-
import pytest
from ..app.utils import sanitize_xml


@pytest.mark.parametrize("input_data,expected", [
    ("text sample", "text sample"),
    ("<p>text sample</p>", "<p>text sample</p>"),
    ("<description>text sample</description>", "<description>text sample</description>"),
    ("<description>text </br> sample</description>", "<description>text </br> sample</description>"),
    ("<p>text </br> sample</p>", "<p>text  sample</p>"),
    ("<p col='a'>text </br> sample</p>", "<p col='a'>text  sample</p>"),
    ("<p>text </br> sample</p><p>other</br></p>", "<p>text  sample</p><p>other</p>"),
    ("<pub>text </br> sample</pub>", "<pub>text </br> sample</pub>"),
    ("<p>text</p><desc>text2</br></desc>", "<p>text</p><desc>text2</br></desc>"),
    ("<pub>text</b></pub><p id=1>text2</br></p>", "<pub>text</b></pub><p id=1>text2</p>"),
    ("<p>text<p>otro</br></p></p>", "<p>textotro</p>"),
])
def test_sanitize_xml(input_data, expected):
    result = sanitize_xml(input_data)
    assert result == expected
